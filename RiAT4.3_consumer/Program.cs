using Confluent.Kafka;
using Newtonsoft.Json;

internal class Program
{
    private static void Main(string[] args)
    {
        Task.Delay(5000).Wait();
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "group1",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("wather");

            var cancellationTokenSource = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cancellationTokenSource.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        ConsumeResult<Ignore, string> consumeResult = consumer.Consume(cancellationTokenSource.Token);
                        WatherData wd = JsonConvert.DeserializeObject<WatherData>(consumeResult.Value);

                        Console.WriteLine($"Температура: {wd.temp}°C (ощущается как {wd.feels_like}°C)");
                        Console.WriteLine($"Скорость ветра: {wd.wind_speed} м/с");
                        Console.WriteLine($"Скорость порывов ветра: {wd.wind_gust} м/с");
                        Console.WriteLine($"Давление: {wd.pressure_mm} мм рт. ст.");
                        Console.WriteLine($"Влажность воздуха: {wd.humidity}%");

                    }
                    catch (ConsumeException exception)
                    {
                        Console.WriteLine($"Ошибка при получении сообщения: {exception.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                //отмена
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}

class WatherData
{
    public int temp { get; set; }
    public int feels_like { get; set; }
    public double wind_speed { get; set; }
    public double wind_gust { get; set; }
    public int pressure_mm { get; set; }
    public int humidity { get; set; }
}
