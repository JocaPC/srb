SET QUOTED_IDENTIFIER OFF;



declare @master_password NVARCHAR(200) = '<Password>',
		@login sysname,
		@password nvarchar(4000),
		@expiry_date datetime = '01/01/2030',
		-- by convention, 4022 is used but any number between 1024 and 32767 is valid.
		@port smallint = 4022;

-- Let's start
DECLARE @c_start_date VARCHAR(20) = CAST(GETUTCDATE() AS VARCHAR(20));
DECLARE @c_expiry_date VARCHAR(20) = CAST(@expiry_date AS VARCHAR(20));
DECLARE @db sysname = DB_NAME();


EXEC("USE master;");

IF NOT EXISTS(SELECT * FROM master.sys.symmetric_keys WHERE NAME = '##MS_DatabaseMasterKey##')
BEGIN
	IF (@master_password IS NULL)
	BEGIN
	PRINT('-- Create MASTER KEY in master database:')
	PRINT('USE master;')
	PRINT('CREATE MASTER KEY ENCRYPTION BY PASSWORD = <Put some strong password here>;')
	GOTO ErrorLabel
	END
	ELSE
		EXEC("CREATE MASTER KEY ENCRYPTION BY PASSWORD = '"+@master_password+"'")
END
ELSE
	PRINT('MASTER KEY exists in master database.')

EXEC("CREATE CERTIFICATE ServiceBrokerCertificate
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
");

EXEC("CREATE ENDPOINT ServiceBrokerEndPoint
	-- set endpoint to activly listen for connections
	STATE = STARTED
	-- set it for TCP traffic only since service broker supports only TCP protocol
	AS TCP (LISTENER_PORT = " + @port +")
	FOR SERVICE_BROKER 
	(
		-- authenticate connections with our certificate
		AUTHENTICATION = CERTIFICATE ServiceBrokerCertificate,
		-- default is REQUIRED encryption but let's just set it to SUPPORTED
		-- SUPPORTED means that the data is encrypted only if the 
		-- opposite endpoint specifies either SUPPORTED or REQUIRED.
		ENCRYPTION = SUPPORTED
	)");

EXEC("GRANT CONNECT ON ENDPOINT::ServiceBrokerEndPoint TO public");


ErrorLabel:

---------------------------------------------
--> Master target setup


EXEC("USE master;") AT [TARGET];

EXEC("IF NOT EXISTS(SELECT * FROM master.sys.symmetric_keys WHERE NAME = '##MS_DatabaseMasterKey##')
BEGIN
	IF (@master_password IS NULL)
	BEGIN
	PRINT('-- Create MASTER KEY in remote master database:')
	PRINT('USE master;')
	PRINT('CREATE MASTER KEY ENCRYPTION BY PASSWORD = <Put some strong password here>;')
	END
	ELSE
		CREATE MASTER KEY ENCRYPTION BY PASSWORD = '"+@master_password+"';
END") AT [TARGET]

EXEC("CREATE CERTIFICATE ServiceBrokerCertificate
WITH 
	-- BOL: The term subject refers to a field in the metadata of 
	--		the certificate as defined in the X.509 standard
	SUBJECT = 'ServiceBrokerCertificate',
	-- set the start date
	START_DATE = '"+@now+"', 
	-- set the expiry data
    EXPIRY_DATE = '"+@expiry_date+"' 
	-- enables the certifiacte for service broker initiator
	ACTIVE FOR BEGIN_DIALOG = ON
") AT [TARGET];

EXEC("CREATE ENDPOINT ServiceBrokerEndPoint
	-- set endpoint to activly listen for connections
	STATE = STARTED
	-- set it for TCP traffic only since service broker supports only TCP protocol
	AS TCP (LISTENER_PORT = " + @port +")
	FOR SERVICE_BROKER 
	(
		-- authenticate connections with our certificate
		AUTHENTICATION = CERTIFICATE ServiceBrokerCertificate,
		-- default is REQUIRED encryption but let's just set it to SUPPORTED
		-- SUPPORTED means that the data is encrypted only if the 
		-- opposite endpoint specifies either SUPPORTED or REQUIRED.
		ENCRYPTION = SUPPORTED
	)") AT [TARGET];




EXEC("CREATE LOGIN "+@login+"Login WITH PASSWORD = '"+@password+"'") AT [TARGET]

-- Create a user for our login
EXEC("CREATE USER "+@login+"User FOR LOGIN "+@login+"Login'") AT [TARGET]

declare @cert_encoded varbinary(max) = CERTENCODED(CERT_ID('ServiceBrokerCertificate')),
		@cert_key varbinary(max) = CERTPRIVATEKEY(CERT_ID('ServiceBrokerCertificate'));


EXEC("CREATE CERTIFICATE ServiceBrokerCertificate 
		AUTHORIZATION "+@login+"User
		FROM BINARY = "+@cert_encoded
		--+"WITH PRIVATE KEY (BINARY = "+@cert_key+")"
) AT [TARGET]

-- finally grant the connect permissions to login for the endpoint
EXEC("GRANT CONNECT ON ENDPOINT::ServiceBrokerEndPoint TO "+@login+"Login") AT [TARGET];



EXEC("USE " + @db);

EXEC("ALTER DATABASE " + @db + " SET ENABLE_BROKER");


select * from sys.routes
