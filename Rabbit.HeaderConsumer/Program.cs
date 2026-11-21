using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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


Console.WriteLine(" [*] Waiting for messages.");


var consumer = new AsyncEventingBasicConsumer(channel);

consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received {message}");
    await Task.Yield();
};
await channel.BasicConsumeAsync(queue: "q.health1",
    //AutoAck means that the message is considered acknowledged as soon as it is delivered to the consumer. If AutoAck is set to false, the consumer must explicitly acknowledge the message after processing it. This allows for more reliable message processing, as messages can be re-queued if the consumer fails before acknowledging them.
    autoAck: true, consumer: consumer);

