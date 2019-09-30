CREATE DATABASE ValidationDB1;
CREATE DATABASE ValidationDB2;

USE ValidationDB1;
alter database current set trustworthy on;
exec srb.create_service 'Sender';

USE ValidationDB2;
exec srb.create_service 'Receiver';

USE ValidationDB1;
exec srb.send_message 'Sender', 'Receiver', 'Hello test';

exec srb.view_messages 'Sender';
select * from sys.transmission_queue;

USE ValidationDB2;
exec srb.view_messages 'Receiver';

declare @msg NVARCHAR(MAX);
exec srb.get_message 'Receiver', @msg output;
if (@msg <> 'Hello test')
	throw 5001, 'Cross-db service broker failed', 1;

USE ValidationDB1;
exec srb.drop_service 'Sender';

USE ValidationDB2;
exec srb.drop_service 'Receiver';
