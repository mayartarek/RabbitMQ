using RabbitMQ.Client;
using System.Text;

Console.WriteLine("Hello, World!");

var factory = new ConnectionFactory
{
    HostName = "localhost",
    Port = 5672,
    UserName = "guest",
    Password = "guest"
};
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();



const string message = "Hello world";

var body = Encoding.UTF8.GetBytes(message);



await channel.BasicPublishAsync<BasicProperties>(
    exchange: "amq.fanout",
    routingKey: string.Empty,
    mandatory: true,
    basicProperties: new BasicProperties(),
    body: body,
    cancellationToken: CancellationToken.None
);

Console.WriteLine($" [x] Sent {message}");
