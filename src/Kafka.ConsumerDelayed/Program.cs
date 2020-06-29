using Confluent.Kafka;
using Kafka.Core.Constants;
using Kafka.Core.Exceptions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.ConsumerDelayed
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var log = Core.Logger.Logger.BuildLogger();

            var cts = new CancellationTokenSource();

            log.Information("Consumindo mensagens do Kafka");

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            var config = new ConsumerConfig
            {
                BootstrapServers = Constants.BootstrapServers,
                GroupId = Constants.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Acks = Acks.All,
                EnableAutoCommit = false
            };

            var configDics = new Dictionary<string, object>
            {
                { "bootstrap.servers", "host1:9092,host2:9092" },
                { "group.id", "foo" },
                { "default.topic.config", new Dictionary<string, object>
                    {
                        { "auto.offset.reset", "smallest" }
                    }
                }
            };

            try
            {

                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(Constants.TopicName);
                    try
                    {
                        while (true)
                        {
                            try
                            {
                                log.Information($"===========================================================");
                                var cr = consumer.Consume(cts.Token);
                                //var partitions = new List<TopicPartition>() { cr.TopicPartition };

                                //consumer.Pause(partitions);

                                //log.Information($"Partition paused. {cr.TopicPartition}");

                                //await Task.Delay(TimeSpan.FromMinutes(30));

                                //consumer.Resume(partitions);
                                
                                //log.Information($"Partition resumed.");

                                var message = cr.Message.Value;

                                await ProcessMessage(message);

                                log.Information($"Mensagem lida: {message}. Topic/Partition: {cr.TopicPartition.Topic} / {cr.TopicPartition.Partition}");

                                consumer.Commit(cr);

                            }
                            catch (KafkaException kex)
                            {

                                log.Error($"Erro: {kex.Message}");
                            }

                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                    }

                }
            }
            catch (Exception ex)
            {
                log.Error($"Erro: {ex.Message}");
            }

        }
        private static async Task ProcessMessage(string message)
        {
            var rnd = new Random();

            var throwException = rnd.Next(1, 5) == 0;

            if (throwException)
            {
                await Task.Delay(5000);
                throw new MyKafkaException($"Simulando uma exceção> Mensagem que estava sendo processada: {message}");
            }
        }
    }
}
