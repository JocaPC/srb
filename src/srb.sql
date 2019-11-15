SET QUOTED_IDENTIFIER OFF; -- Because I use "" as a string literal
GO

CREATE SCHEMA srb;
GO

CREATE TYPE srb.Strings AS TABLE( text NVARCHAR(MAX) ); 
GO
CREATE TYPE srb.Integers AS TABLE( val INT ); 
GO
CREATE TYPE srb.Messages AS TABLE(  
     sequence_number BIGINT,
	 data VARBINARY(MAX),
	 text AS CAST(data AS NVARCHAR(MAX)),
     type NVARCHAR(256), 
     dialog UNIQUEIDENTIFIER,  
     contract NVARCHAR(256),    
     validation NCHAR,
	 service NVARCHAR(512),  
	 service_instance_id UNIQUEIDENTIFIER 
); 
GO  

-- Start Service management
CREATE OR ALTER PROCEDURE srb.create_service_on_queue
@name sysname, 
@queue sysname, 
@contract sysname = '[DEFAULT]',
@whatif bit = 1
AS BEGIN
-- Creates a Service on the existing queue. Will use 'DefaultContact' if the contract is not specified.
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = CONCAT('CREATE SERVICE ', @name, ' ON QUEUE ', @queue, ' (', @contract, ')');
	IF(@whatif = 1)
	PRINT(@sql);
	ELSE
	EXEC(@sql);
END;
GO

CREATE OR ALTER PROCEDURE srb.create_service 
@name sysname, 
@callback nvarchar(256) = NULL, 
@contract sysname = '[DEFAULT]',
@trigger nvarchar(256) = NULL,
@use_transaction bit = 1,
@remus bit = 0,
@whatif bit = 0
AS BEGIN

	-- Creates a Service including a new queue. Will use 'DefaultContact' if the contract is not specified.
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @queue SYSNAME = @name+'Queue';

	IF (@callback IS NOT NULL) BEGIN

		if( 0 =
		(select count(*) from sys.objects o		
			where (schema_name(o.schema_id) + '.' + object_name(o.object_id)) = @callback)
		)
		begin 
			declare @error1 nvarchar(4000) = 'Cannot find procedure ' + @callback;
			throw 60001, @error1, 1; 
		end

		-- Checking the signature (parameter types) of callback procedure.
		if( 
		(select count(*)
		from sys.parameters p
			join sys.objects o
				on p.object_id = o.object_id
			join sys.types t
				on p.system_type_id = t.system_type_id
		where (schema_name(o.schema_id) + '.' + object_name(o.object_id)) = @callback
		and (
			p.parameter_id = 1 and p.user_type_id in (165, 231) -- varbinary, nvarchar
			or
			p.parameter_id = 2 and p.system_type_id = 231 -- nvarchar, sysname
			or
			p.parameter_id = 3 and p.system_type_id = 36 -- uniqueidentifier
		)) <= 3)
		begin 
			declare @error2 nvarchar(4000) = 'Procedure ' + @callback + ' exists, but it don''t have parameters (varbinary(max), nvarchar, uniqueidentifier)';
			throw 60001, @error2, 2; 
		end
	END;

	SET @sql = CONCAT('CREATE QUEUE ', @queue, ' WITH STATUS = ON');

	IF(@whatif = 1) BEGIN
	PRINT(@sql);
	PRINT('
GO')
	END ELSE
	EXEC(@sql);

	IF (@callback IS NOT NULL OR @trigger IS NOT NULL) BEGIN

		DECLARE @msgtype varchar(30) = (select top 1 case t.name when 'xml' then 'xml' else t.name + '(max)' end
		from sys.parameters p
			join sys.objects o
				on p.object_id = o.object_id
			join sys.types t
				on p.system_type_id = t.system_type_id
		where (schema_name(o.schema_id) + '.' + object_name(o.object_id)) = @callback
		and parameter_id = 1
		and t.user_type_id in (165, 231));

		PRINT '--> Callback message type is ' + @msgtype;

		SET @sql = CONCAT("
CREATE PROCEDURE ",@queue,"ActivationProcedure
AS BEGIN",
IIF(@remus = 1, "

DECLARE @tableMessages TABLE (

queuing_order BIGINT,

conversation_handle UNIQUEIDENTIFIER,

message_type_name SYSNAME,

message_body VARBINARY(MAX));

-- Create cursor over the table variable
-- Use the queueing_order column to
-- preserve the message order

DECLARE cursorMessages

CURSOR FORWARD_ONLY READ_ONLY

FOR SELECT conversation_handle,

message_type_name,

message_body

FROM @tableMessages

ORDER BY queuing_order;

", "")
,"
	DECLARE @dialog UNIQUEIDENTIFIER;
	DECLARE @msg VARBINARY(MAX);
	DECLARE @msg_type sysname;
",     
	IIF(@remus=1, "", "WHILE (1=1)"), -- Remus is not using WHILE around RECEIVE
	"
	BEGIN
		",     
		IIF(@use_transaction=1, "BEGIN TRANSACTION;", "/*No transaction*/")
,"
		WAITFOR (
			RECEIVE ",
			IIF(@remus = 0, " TOP(1) 
				@dialog = conversation_handle,
				@msg = message_body,
				@msg_type = message_type_name ",
				-- Remus will take them all
				" queuing_order, conversation_handle, message_type_name, message_body"), "
			FROM ",@queue,
			IIF(@remus = 1, " INTO @tableMessages ", ""), -- Remus will put all into table variable
			"
		), TIMEOUT 5000;
",
		-- Remus don't needs transaction/break
		IIF(@remus=0, "IF (@@ROWCOUNT = 0)
		BEGIN
			"+     
			IIF(@use_transaction=1, "ROLLBACK TRANSACTION;", "/*No transaction*/")+
			" BREAK; 
		END",""),

         IIF(@remus=0,"","
		 -- Remus will open cursor:
		 OPEN cursorMessages;

		WHILE (1=1)

		BEGIN

		FETCH NEXT FROM cursorMessages

		INTO @dialog, @msg_type, @msg;

		IF (@@FETCH_STATUS != 0)

		BREAK;
		 
		 ")
		,"IF @msg_type =
				N'http://schemas.microsoft.com/SQL/ServiceBroker/DialogTimer'
		BEGIN
			BEGIN TRY ",
			CASE 
				WHEN @trigger IS NOT NULL THEN
				CONCAT("-- Execute a timeout callback and provide message info.
				EXEC ", @trigger, " @dialog;")
				ELSE CONCAT(" -- Log timeout message
				RAISERROR( 'Timeout in service ",@name, "', 6, 1) WITH LOG; ")
			END,"
			END TRY  
			BEGIN CATCH  
				
				DECLARE @errorTimeout NVARCHAR(MAX) = ( SELECT   
					ERROR_NUMBER() AS ErrorNumber  
					,ERROR_SEVERITY() AS Severity  
					,ERROR_STATE() AS State  
					,ERROR_PROCEDURE() AS [Procedure]  
					,ERROR_LINE() AS Line  
					,ERROR_MESSAGE() AS [Error]
				FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);

				RAISERROR( @errorTimeout, 6, 1) WITH LOG;
            
				IF @@TRANCOUNT > 0  BEGIN
					ROLLBACK TRANSACTION;
					BREAK;
				END
			END CATCH;  
		END
		ELSE IF @msg_type =
				N'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
		BEGIN
				END CONVERSATION @dialog;
		END
		ELSE IF @msg_type =
				N'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
		BEGIN
				DECLARE @c NVARCHAR(100);
				DECLARE @errorMsg NVARCHAR(4000);
				SET @c = CAST(@dialog AS NVARCHAR(100));
				SET @errorMsg = CAST(@msg AS NVARCHAR(MAX));
				RAISERROR (N'Conversation %s was ended with error %s', 6, 1, @c, @errorMsg) WITH LOG;
				END CONVERSATION @dialog;
		END
		ELSE 
		BEGIN  
			BEGIN TRY",  
			CASE 
				WHEN @callback IS NOT NULL THEN
				CONCAT("-- Execute a callback and provide message info.
				DECLARE @p ",@msgtype," = CAST(@msg AS ",@msgtype,");
				EXEC ", @callback, " @p, @msg_type, @dialog;")
				ELSE CONCAT(" -- Log message
				RAISERROR( 'Un-processed message in service ",@name, "', 6, 1) WITH LOG; ")
			END,"	
			END TRY  
			BEGIN CATCH  
				
				DECLARE @error NVARCHAR(MAX) = ( SELECT   
					ERROR_NUMBER() AS ErrorNumber  
					,ERROR_SEVERITY() AS Severity  
					,ERROR_STATE() AS State  
					,ERROR_PROCEDURE() AS [Procedure]  
					,ERROR_LINE() AS Line  
					,ERROR_MESSAGE() AS [Error]
				FOR JSON PATH, WITHOUT_ARRAY_WRAPPER);

				RAISERROR( @error, 6, 1) WITH LOG;
            
				IF @@TRANCOUNT > 0  BEGIN
					ROLLBACK TRANSACTION;
					BREAK;
				END
			END CATCH;  
		END  
		IF @@TRANCOUNT > 0
			COMMIT TRANSACTION;",
IIF(@remus = 0, "",
"		CLOSE cursorMessages;

		DELETE FROM @tableMessages;

		IF @@TRANCOUNT > 0
			COMMIT TRANSACTION;

		END

		DEALLOCATE cursorMessages;"
)

,"
	END --WHILE 1=1
END");
		IF(@whatif = 1) BEGIN
		PRINT(@sql);
		PRINT('
GO')
		END ELSE
		EXEC(@sql);

		SET @sql = CONCAT("ALTER QUEUE ", @queue, " WITH
		ACTIVATION (  
				STATUS = ON,   
				PROCEDURE_NAME = ",@queue,"ActivationProcedure,
				EXECUTE AS 'dbo',
				MAX_QUEUE_READERS = 1)");

		IF(@whatif = 1) BEGIN
		PRINT(@sql);
		PRINT('
GO')
		END ELSE
		EXEC(@sql);

	END -- callback
		
    EXEC srb.create_service_on_queue @name, @queue, @contract, @whatif;

END
GO

CREATE OR ALTER PROCEDURE srb.drop_service 
@name sysname
AS BEGIN
    DECLARE @sql NVARCHAR(MAX);
	
    SET @sql = CONCAT('DROP PROCEDURE IF EXISTS ', @name, 'QueueActivationProcedure');
	EXEC(@sql);

	BEGIN TRY
    SET @sql = CONCAT('DROP SERVICE ', @name);
	EXEC(@sql);
	END TRY BEGIN CATCH END CATCH;

	BEGIN TRY
-- @bug: drop it only if exists and if there are no other services on the same queue.
	SET @sql = CONCAT('DROP QUEUE ', @name, 'Queue');
    EXEC(@sql);
	END TRY BEGIN CATCH END CATCH;
END
GO
-- /END Service management.

-- START Dialog/Conversation management.

GO
CREATE OR ALTER FUNCTION srb.get_cached_dialog (
@sender sysname,
@receiver sysname)
RETURNS UNIQUEIDENTIFIER
AS BEGIN
	DECLARE @dialog_handle UNIQUEIDENTIFIER = NULL;
	
	SELECT @dialog_handle = conversation_handle
	FROM sys.conversation_endpoints ce
	JOIN sys.services s ON ce.service_id = s.service_id
	WHERE is_initiator = 1
	AND ce.far_service = @receiver
	AND s.name = @sender;
	
	RETURN (@dialog_handle);
END
GO

CREATE OR ALTER PROCEDURE srb.new_conversation
@conversation_handle UNIQUEIDENTIFIER OUTPUT,
@sender sysname,
@receiver sysname,
@contract sysname = '[DEFAULT]',
@encryption varchar(3) = 'OFF' --> Keep it off unless if you know how to setup master keys.
AS BEGIN
    DECLARE @sql NVARCHAR(MAX);

	SET @sql = CONCAT(
"BEGIN DIALOG @id
FROM SERVICE [", @sender, "]
TO SERVICE '", @receiver, "'
ON CONTRACT ", @contract,"
WITH ENCRYPTION = ", @encryption);

    EXEC sp_executesql @sql, N'@id uniqueidentifier OUTPUT', @id = @conversation_handle OUTPUT;
END
GO

CREATE OR ALTER PROCEDURE srb.ping
@conversation_handle UNIQUEIDENTIFIER,
@delay_s bigint NULL
AS BEGIN
	BEGIN CONVERSATION TIMER (@conversation_handle) TIMEOUT = @delay_s; 
END
GO

CREATE OR ALTER PROCEDURE srb.start_dialog
@sender sysname,
@receiver sysname,
@contract sysname = '[DEFAULT]',
@encryption varchar(3) = 'OFF' --> Keep it off unless if you know how to setup master keys.
AS BEGIN
	DECLARE @dialog UNIQUEIDENTIFIER; -- Just to ignore it.
	EXEC srb.new_conversation
		@conversation_handle = @dialog OUTPUT,
		@sender = @sender,
		@receiver = @receiver,
		@contract = @contract,
		@encryption = @encryption 
	--PRINT @dialog;
END
GO

CREATE SYNONYM srb.start_conversation FOR srb.start_dialog;
GO

CREATE OR ALTER PROCEDURE srb.trigger_dialog
@from SYSNAME,
@to SYSNAME,
@delay_s bigint NULL
AS BEGIN
	DECLARE @dialog UNIQUEIDENTIFIER;
	SET @dialog = srb.get_cached_dialog(@from, @to);

	IF (@dialog IS NULL) BEGIN
		--PRINT 'Verbose: Starting new dialog';
		EXEC srb.new_conversation @dialog OUTPUT, @from, @to;
	END

	--PRINT @dialog; 
	BEGIN CONVERSATION TIMER (@dialog) TIMEOUT = @delay_s; 
END
GO

CREATE OR ALTER PROCEDURE srb.drop_conversation
@conversation_handle UNIQUEIDENTIFIER,
@cleanup bit = 0
AS BEGIN
	IF (@cleanup = 0)
	END CONVERSATION @conversation_handle;
	ELSE
	END CONVERSATION @conversation_handle WITH CLEANUP;
END
GO
CREATE SYNONYM  srb.drop_dialog FOR srb.drop_conversation;
GO
CREATE SYNONYM  srb.delete_dialog FOR srb.drop_conversation;
GO
CREATE SYNONYM  srb.delete_conversation FOR srb.drop_conversation;
GO

CREATE OR ALTER PROCEDURE srb.end_dialog
@sender sysname,
@receiver sysname,
@cleanup bit = 0
AS BEGIN
	DECLARE @dialog_handle UNIQUEIDENTIFIER;
	SET @dialog_handle = srb.get_cached_dialog(@sender, @receiver);
	EXEC srb.drop_conversation @dialog_handle, @cleanup;
END
GO

CREATE SYNONYM srb.end_conversation FOR srb.end_dialog;
GO

-- /END Dialog/Conversation management.

CREATE OR ALTER PROCEDURE srb.post_message
@dialog UNIQUEIDENTIFIER,
@message NVARCHAR(MAX)
AS BEGIN
	DECLARE @content VARBINARY(MAX) = CAST(@message AS VARBINARY(MAX));
    SEND ON CONVERSATION @dialog (@content); 
END
GO

CREATE OR ALTER PROCEDURE srb.post_data
@dialog UNIQUEIDENTIFIER,
@data VARBINARY(MAX)
AS BEGIN;
    SEND ON CONVERSATION @dialog (@data); 
END
GO

CREATE OR ALTER PROCEDURE srb.send_message
@from SYSNAME,
@to SYSNAME,
@message NVARCHAR(MAX)
AS BEGIN
	DECLARE @dialog UNIQUEIDENTIFIER;
	SET @dialog = srb.get_cached_dialog(@from, @to);

	IF (@dialog IS NULL) BEGIN
		--PRINT 'Verbose: Starting new dialog';
		EXEC srb.new_conversation @dialog OUTPUT, @from, @to;
	END

	--PRINT @dialog; 
	DECLARE @content VARBINARY(MAX) = CAST(@message AS VARBINARY(MAX));
	SEND ON CONVERSATION @dialog (@content);
END
GO

CREATE OR ALTER PROCEDURE srb.send_data
@from SYSNAME,
@to SYSNAME,
@message VARBINARY(MAX)
AS BEGIN
		DECLARE @content VARCHAR(MAX) = CAST(@message AS VARCHAR(MAX));
		EXEC srb.send_message @from, @to, @content;
END
GO

CREATE OR ALTER PROCEDURE srb.get_message @service sysname, @message NVARCHAR(MAX) OUTPUT
AS BEGIN
	  
	DECLARE @sql NVARCHAR(MAX);

	SET @sql = CONCAT("
DECLARE @t srb.Strings;
RECEIVE TOP (1) cast(message_body as nvarchar(max)) as text
FROM ", @service, "Queue
INTO @t;
SELECT @var = text FROM @t;
	");

	EXEC sp_executesql @sql, N'@var NVARCHAR(MAX) OUTPUT', @var = @message OUTPUT;
END
GO

CREATE OR ALTER PROCEDURE srb.view_messages @service sysname
AS BEGIN
	DECLARE @sql NVARCHAR(MAX) = CONCAT('SELECT text = cast(message_body as nvarchar(max)), message_type_name, conversation_handle, message_sequence_number FROM ', @service, 'Queue ORDER BY queuing_order ASC');
	EXEC(@sql);
END
GO

CREATE OR ALTER PROCEDURE srb.view_data @service sysname
AS BEGIN
	DECLARE @sql NVARCHAR(MAX) = CONCAT('SELECT message_body, message_type_name, conversation_handle, message_sequence_number FROM ', @service, 'Queue ORDER BY queuing_order ASC');
	EXEC(@sql);
END
GO

CREATE OR ALTER PROCEDURE srb.get_messages @service sysname, @res srb.Messages READONLY, @count int = 1
AS BEGIN;
	DECLARE @sql NVARCHAR(MAX);

	SET @sql = CONCAT("
	RECEIVE TOP (@N) * 
	FROM ", @service,"Queue
	--INTO @messages
	");

	EXEC sp_executesql 
	@sql, --N'@messages srb.Messages OUTPUT,@N int',
						N'@N int',
						--@messages = @res, 
						@N = @count
		;
END
GO


create or alter procedure
srb.enable_remote_access 
		-- by convention, 4022 is used but any number between 1024 and 32767 is valid.
		@port smallint = 4022,
		@start_date datetime = NULL,
		@expiry_date datetime = NULL,
		@master_password NVARCHAR(200) = NULL,
		@remote_login NVARCHAR(200) = 'public',
		@what_if bit = 1
as begin

DECLARE @c_start_date VARCHAR(20) = CAST(ISNULL(@start_date, DATEADD(day, -1, GETUTCDATE())) AS VARCHAR(30));
DECLARE @c_expiry_date VARCHAR(20) = CAST(ISNULL(@expiry_date, DATEADD(year, 1, GETUTCDATE())) AS VARCHAR(30));
DECLARe @sql NVARCHAR(MAX);

IF NOT EXISTS(SELECT * FROM master.sys.symmetric_keys WHERE NAME = '##MS_DatabaseMasterKey##')
BEGIN
	IF (@master_password IS NULL)
	BEGIN
	PRINT('-- MASTER KEY is not found in master database!')
	PRINT('-- Specify parameter @master_pasword, or')
	PRINT('-- Create MASTER KEY in master database:')
	PRINT('/*
	USE master;')
	PRINT('CREATE MASTER KEY ENCRYPTION BY PASSWORD = <Put some strong password here>;
	*/')
	GOTO ErrorLabel
	END
	ELSE BEGIN
		IF (@what_if = 0)
		EXEC("USE master;CREATE MASTER KEY ENCRYPTION BY PASSWORD = '"+@master_password+"'")
		ELSE
		PRINT("USE master;CREATE MASTER KEY ENCRYPTION BY PASSWORD = '"+@master_password+"'")
	END
END
ELSE
	PRINT('-- MASTER KEY exists in master database.')


SET @sql = "USE master;
CREATE CERTIFICATE ServiceBrokerCertificate
WITH 
	-- BOL: The term subject refers to a field in the metadata of 
	--		the certificate as defined in the X.509 standard
	SUBJECT = 'ServiceBrokerCertificate',
	-- set the start date 
	START_DATE = '"+@c_start_date+"', 
	-- set the expiry data
    EXPIRY_DATE = '"+@c_expiry_date+"'
	-- enables the certifiacte for service broker initiator
	ACTIVE FOR BEGIN_DIALOG = ON
";
PRINT '-- Creating Service Broker certificate...';

IF @what_if = 0	EXEC(@sql);
	ELSE		PRINT (@sql);

SET @sql = "USE master;
CREATE ENDPOINT ServiceBrokerEndPoint
	-- set endpoint to activly listen for connections
	STATE = STARTED
	-- set it for TCP traffic only since service broker supports only TCP protocol
	AS TCP (LISTENER_PORT = " + CAST(@port AS VARCHAR(6)) +")
	FOR SERVICE_BROKER 
	(
		-- authenticate connections with our certificate
		AUTHENTICATION = CERTIFICATE ServiceBrokerCertificate,
		-- default is REQUIRED encryption but let's just set it to SUPPORTED
		-- SUPPORTED means that the data is encrypted only if the 
		-- opposite endpoint specifies either SUPPORTED or REQUIRED.
		ENCRYPTION = SUPPORTED
	)";
PRINT '-- Creating Service Broker endpoint...';

IF @what_if = 0	EXEC(@sql);
	ELSE		PRINT (@sql);

PRINT '-- Enabling login ' + @remote_login + ' to access Service Broker endpoint...';
IF @remote_login IS NOT NULL
	IF @what_if = 0
			EXEC("USE master;GRANT CONNECT ON ENDPOINT::ServiceBrokerEndPoint TO "+@remote_login+";");
	ELSE 	PRINT("USE master;GRANT CONNECT ON ENDPOINT::ServiceBrokerEndPoint TO "+@remote_login+";")

ErrorLabel:
end
go

CREATE PROCEDURE
srb.remove_remote_access
AS BEGIN

	declare @service_broker_endpoint sysname, @certificate_name sysname;
	declare @sql nvarchar(max);
	select @service_broker_endpoint = sbe.name, @certificate_name = c.name
	from sys.service_broker_endpoints sbe
	left join master.sys.certificates c on sbe.certificate_id = c.certificate_id

	if(@service_broker_endpoint is not null)
	begin
		set @sql = 'USE master;DROP ENDPOINT ' + @service_broker_endpoint;
		exec(@sql);
		PRINT '-- Dropped service broker endpoint in master database';
	end
	else
		print '-- ServiceBroker endpoint don''t exists.';

	if(@certificate_name is not null)
	begin
		set @sql = 'USE master;DROP CERTIFICATE ' + @certificate_name;
		exec(@sql);
		PRINT 'Dropped service broker certificate in master database';
	end
	else
		print 'Certificate for ServiceBroker endpoint don''t exists';

END
GO

-- Procedure that generates script that will initialize login on remote sender instance to access this instance.
-- Make sure that you have enabled remote access on the instances.
CREATE OR ALTER PROCEDURE
srb.init_remote_proxy_login 
		@login sysname,
		@password nvarchar(4000),
		@what_if bit = 1
as begin

declare @sql nvarchar(max) = '
USE master;';

-- create the login on remote instance that will be send the data through the Endpoint
set @sql += "CREATE LOGIN "+@login+" WITH PASSWORD = '"+@password+"';
";
-- Create a user for our login
set @sql += "CREATE USER "+@login+"User FOR LOGIN "+@login+";
";

declare @cert_encoded varbinary(max);
--EXEC sp_executesql	N'USE master;SELECT @cert = CERTENCODED(certificate_id) from sys.service_broker_endpoints',
--					N'@cert VARBINARY(MAX) OUTPUT',
--					@cert = @cert_encoded OUTPUT;

SELECT @cert_encoded = CERTENCODED(certificate_id)
from mastersys.service_broker_endpoints;

if(@cert_encoded is not null)
set @sql += "CREATE CERTIFICATE "+@login+"ProxyServiceBrokerCertificate 
		AUTHORIZATION "+@login+"User
		FROM BINARY = "+CONVERT(VARCHAR(MAX), @cert_encoded, 1)+";
";
else
	print '--> Cannot find the certificate for service endpoint. Run EXEC srb.enable_remote_access'

-- finally grant the connect permissions to login for the endpoint
set @sql += "
go
declare @endpoint_name sysname;
select @endpoint_name = name
from sys.service_broker_endpoints sbe;

-- Enable sender login on remote (sender) instance to connect to sender endpoint:
declare @sql = 'GRANT CONNECT ON ENDPOINT::' +@endpoint_name+ ' TO "+@login+"';

EXEC(sql);
";

PRINT '--> Execute the following script on the remote server instance:'
PRINT @sql

end
GO

-- Initialize route on the sender instance.
CREATE PROCEDURE srb.init_remote_proxy_route 
	@service SYSNAME,
	@address varchar(256) = NULL,
	@authorization sysname = 'dbo'
AS BEGIN
declare @sql NVARCHAR(MAX);

with route_info as (
select db_name = DB_NAME(DB_ID()), service_name = @service,
		sb_guid = (select service_broker_guid from sys.databases where name = DB_NAME(DB_ID())), 
		protocol = te.protocol_desc,
		server = isnull(ip_address, CAST(serverproperty('servername') as varchar(256))),
		port
from sys.service_broker_endpoints sbe
	join sys.tcp_endpoints te on sbe.endpoint_id = te.endpoint_id
where sbe.type = 3 -- SERVICE_BROKER
and EXISTS(SELECT * FROM sys.services WHERE name = @service)
)
select @sql = CONCAT("CREATE ROUTE [", server, "/", db_name, "/", service_name , "] AUTHORIZATION [",@authorization,"] 
WITH SERVICE_NAME = N'",service_name,"' ,
		BROKER_INSTANCE = N'",sb_guid,"' , 
		ADDRESS = N'",
		ISNULL(@address, CONCAT(protocol,"://",server,":",port)),
		"'-- OR 'LOCAL' for intra-instance routes.")
from route_info;

PRINT 'Execute the following script on the remote server instance:'
PRINT @sql
END;
GO

CREATE OR ALTER VIEW srb.conversations AS
SELECT conversation_handle, source = s.name, target = far_service, 
		target_instance = far_broker_instance, state = state_desc,
		target_queue = sq.name,
		target_activation_procedure = sq.activation_procedure
FROM sys.conversation_endpoints ce
JOIN sys.services s ON ce.service_id = s.service_id
JOIN sys.service_queues sq ON s.service_queue_id = sq.object_id;
GO

CREATE OR ALTER PROCEDURE srb.kill_all_conversations
AS BEGIN

DECLARE @conversation_handle UNIQUEIDENTIFIER;
DECLARE conversation_cursor CURSOR FOR  
SELECT DISTINCT conversation_handle FROM srb.conversations;  
  
OPEN conversation_cursor;  
    
FETCH NEXT FROM conversation_cursor
INTO @conversation_handle;  
  
-- Check @@FETCH_STATUS to see if there are any more rows to fetch.  
WHILE @@FETCH_STATUS = 0  
BEGIN  
   EXEC srb.drop_conversation @conversation_handle;
   FETCH NEXT FROM conversation_cursor
   INTO @conversation_handle;  
END  
  
CLOSE conversation_cursor;  
DEALLOCATE conversation_cursor;  

END