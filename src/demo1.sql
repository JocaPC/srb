--ALTER AUTHORIZATION ON DATABASE::<database name> TO [sa];

exec srb.create_service 'Sender';
exec srb.create_service 'Receiver';

exec srb.view_messages 'Sender';
exec srb.view_messages 'Receiver';

exec srb.send_message 'Sender', 'Receiver', 'Hello, this is my first mesage';

exec srb.view_messages 'Sender';
exec srb.view_messages 'Receiver';

select * from sys.transmission_queue;

exec srb.drop_service 'Sender';
exec srb.drop_service 'Receiver';
go
