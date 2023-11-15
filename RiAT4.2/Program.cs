using Confluent.Kafka;
using RestSharp;

internal class Program
{
    private static async Task Main(string[] args)
    {
        ProducerConfig config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using (IProducer<Null, string> producer = new ProducerBuilder<Null, string>(config).Build())
        {
            if (IsBrokerAvailable(config))
            {
                string topic = "wather";
                string strMessage = await GetWatherData();

                var message = new Message<Null, string> { Value = strMessage };

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

    static async Task<string> GetWatherData()
    {
        var options = new RestClientOptions("https://api.weather.yandex.ru/v2/forecast/?lat=55.48628796307604&lon=28.76350062509225&lang=ru_RU");
        var client = new RestClient(options);
        var request = new RestRequest("");
        request.AddHeader("X-Yandex-API-Key", "ТУТ КЛЮЧ ОТ API ЯНДЕКС ПОГОДЫ"); //!!!!!!!!!!!!
        var response = await client.GetAsync(request);
        Console.WriteLine("Данные о погоде получены");

        string strResponce = response.Content;
        int startIndex = strResponce.IndexOf("fact") + 6;
        int endIndex = strResponce.IndexOf("\"forecasts\"") - 1;
        strResponce = strResponce.Substring(startIndex, endIndex - startIndex);

        return strResponce;
    }

}