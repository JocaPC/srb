go
create or alter procedure dbo.send_current_time @dialog uniqueidentifier
as begin
	declare @msg NVARCHAR(MAX) = 'Current time is: ' + CONVERT(VARCHAR(30), GETDATE(), 108);
	exec srb.post_message @dialog, @msg;
	exec srb.ping @dialog, 5; -- trigger the dialog again after 5 seconds
end;
go
exec srb.create_service 'Sender', @trigger = 'dbo.send_current_time';
go
exec srb.create_service 'Receiver';


exec srb.trigger_dialog 'Sender', 'Receiver', 10

exec srb.view_messages 'Receiver';

-- wait 10 seconds and then repeat this:
exec srb.view_messages 'Receiver';


-- Look at dialog_timer column (in UTC time) to see when will the job be run next
SELECT conversation_handle, dialog_timer, state_desc, far_service FROM sys.conversation_endpoints;


exec srb.end_dialog 'Sender', 'Receiver', 1;
exec srb.drop_service 'Sender';
exec srb.drop_service 'Receiver';