using Confluent.Kafka;
using System;

class Program
{
    static void Main(string[] args)
    {
        ProducerConfig config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(config).Build())
        {
            if (IsBrokerAvailable(config))
            {
                string topic = "my-topic";
                string strMessage = string.Empty;

                while (true)
                {
                    Console.Write("Сообщение: ");
                    strMessage = Console.ReadLine();

                    if (string.IsNullOrEmpty(strMessage))
                    {
                        Console.WriteLine("\nОШИБКА пустой текс сообщения\n");
                        continue;
                    }

                    break;
                }

                var message = new Message<Null, string> { Value = "Hello, Kafka!" };

                producer.Produce(topic, message, deliveryReport =>
                {
                    if (deliveryReport.Error.Code == ErrorCode.NoError)
                    {
                        Console.WriteLine($"Сообщение успешно доставлено");
                    }
                    else
                    {
                        Console.WriteLine($"Ошибка доставки сообщения: {deliveryReport.Error.Reason}");
                    }
                });

                producer.Flush(TimeSpan.FromSeconds(10));

            }
        }
    }

    static bool IsBrokerAvailable(ProducerConfig config)
    {
        using (var adminClient = new AdminClientBuilder(config).Build())
        {
            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                Console.WriteLine($"Брокер доступен.\nOriginatingBrokerName: {metadata.OriginatingBrokerName}");
                return true;
            }
            catch (KafkaException)
            {
                Console.WriteLine("Брокер не доступен.");
                return false;
            }
        }
    }
}