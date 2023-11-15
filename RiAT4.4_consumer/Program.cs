using Confluent.Kafka;

internal class Program
{
    private static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "group2",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            consumer.Subscribe("temperature-events");

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
                        List<TopicPartition> tPartitions = new List<TopicPartition>();
                        tPartitions.Add(consumeResult.TopicPartition);

                        string data = consumeResult.Message.Value;
                        string thermName = data.Substring(0, data.IndexOf(':') + 1);
                        double thermValue = double.Parse(data.Substring(data.IndexOf(":") + 1));

                        if (thermValue >= 80.0)
                            Console.ForegroundColor = ConsoleColor.Red;
                        TimeOnly now = TimeOnly.FromDateTime(DateTime.Now);
                        Console.WriteLine($"{now.Hour}:{(now.Minute < 10 ? "0"+now.Minute.ToString():now.Minute)}:{(now.Second < 10? "0"+now.Second.ToString(): now.Second)}\t{thermName}: {thermValue}°C");
                        Console.ForegroundColor = ConsoleColor.White;

                        consumer.Pause(tPartitions);
                        Task.Delay(1000).Wait();
                        consumer.Resume(tPartitions);
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