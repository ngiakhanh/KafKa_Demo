using Confluent.Kafka;
using System;

namespace Kafka_Producer
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var conf = new ProducerConfig { BootstrapServers = "localhost:9092" };

            void Handler(DeliveryReport<Null, string> r) =>
                Console.WriteLine(!r.Error.IsError
                    ? $"Delivered message to {r.TopicPartitionOffset}"
                    : $"Delivery Error: {r.Error.Reason}");

            using (var p = new ProducerBuilder<Null, string>(conf).Build())
            {
                Console.WriteLine("Hi, I am producer.");

                while (true)
                {
                    try
                    {
                        var mess = Console.ReadLine();
                        if (!string.IsNullOrWhiteSpace(mess))
                        {
                            p.Produce("timemanagement_booking", new Message<Null, string> {Value = mess}, Handler);
                            p.Flush(TimeSpan.FromSeconds(10));
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Error occured: {e.Message}");
                    }
                }
            }
        }
    }
}