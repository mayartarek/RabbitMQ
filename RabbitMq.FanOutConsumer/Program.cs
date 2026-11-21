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
for(int i = 1; i <= 5; i++)
{
    var queueName = $"Queue_fanout{i}";
    await channel.QueueDeclareAsync(queue: queueName,
        durable: true,
        exclusive: false,
        autoDelete: false,
        arguments: null);
    await ConsumeMessagesAsync(queueName);
}

Console.WriteLine(" [*] Waiting for messages.");

async  Task ConsumeMessagesAsync(string queueName)
{
    var consumer = new AsyncEventingBasicConsumer(channel);
    consumer.ReceivedAsync += async (model, ea) =>
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        Console.WriteLine($" [x] Received {message} From {queueName}" );
        await Task.Yield();
    };
    await channel.BasicConsumeAsync(queue: queueName,
        //AutoAck means that the message is considered acknowledged as soon as it is delivered to the consumer. If AutoAck is set to false, the consumer must explicitly acknowledge the message after processing it. This allows for more reliable message processing, as messages can be re-queued if the consumer fails before acknowledging them.
        autoAck: true, consumer: consumer);
}

