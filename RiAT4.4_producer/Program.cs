using Confluent.Kafka;
using RiAT4._4_producer;

internal class Program
{
    private static async Task Main(string[] args)
    {
        Console.WriteLine("Producer");
        ProducerConfig config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(config).Build())
        {
            object locker = new object();
            string topic = "temperature-events";

            List<Thermometer> thermometers = new List<Thermometer>();

            while (thermometers.Count < 10)
            {
                Thermometer th = new Thermometer($"term{thermometers.Count + 1}", (value) =>
                {
                    Message<Null, string> message = new Message<Null, string> { Value = $"{value.name}:{value.Value}" };

                    lock (locker)
                    {
                        producer.Produce(topic, message);
                    }
                });

                thermometers.Add(th);
            }

            foreach (var th in thermometers)
                th.Start();

            while (true)
            {
                //ждем
            }

        }
    }
}