use DB1
go
-- Cleanup: drops created services             
exec srb.drop_service 'EchoService'
exec srb.drop_service 'SenderService'
go
----------------------
--- Initialization ---
----------------------
--> Creates a service that will send a message:
exec srb.create_service 'SenderService';
go

--> Creates a service with an activation procedure that will be called once it receives a message.
--> EchoService just replies in the format 'Hello <name>'
create or alter procedure dbo.reply @data NVARCHAR(MAX), @type nvarchar(512), @dialog UNIQUEIDENTIFIER
as begin
	declare @reply NVARCHAR(MAX) = 'Hello ' + @data;
	exec srb.post_message @dialog, @reply;
end;
go
exec srb.create_service 'EchoService', 'dbo.reply';


----------------------
-------- Demo --------
----------------------

-- Sends the message 'Jovan' via dialog between Sender and EchoService

declare @dialog uniqueidentifier;
exec srb.start_dialog @dialog OUTPUT, 'SenderService', 'EchoService';
print @dialog;

exec srb.post_message @dialog, N'Jovan'
exec srb.post_message @dialog, N'Mike'
exec srb.post_message @dialog, N'Hrkljush'

--> EchoService will reply to Sender.

-- Looks inside the 'SenderService' service to see the reply.
exec srb.view_messages 'SenderService'
exec srb.view_messages 'EchoService'
select * from sys.transmission_queue

-- Takes the message from the 'SenderService' service
declare @msg NVARCHAR(MAX);
exec srb.get_message 'SenderService', @msg output;
select 'Replied mesage: ' + @msg

-- Checks whether there's some message in the service. There should be none.
exec srb.view_messages 'SenderService'

exec srb.end_conversation 'SenderService', 'EchoService';

exec srb.view_messages 'SenderService'

-- Cleanup: drops created services             
exec srb.drop_service 'EchoService'
exec srb.drop_service 'SenderService'
