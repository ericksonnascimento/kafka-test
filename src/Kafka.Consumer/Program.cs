using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Core.Constants;
using Kafka.Core.Exceptions;

namespace Kafka.Consumer
{
    class Program
    {
        private static Serilog.Core.Logger _logger;

        static async Task Main(string[] args)
        {
            _logger = Core.Logger.Logger.BuildLogger();

            var cts = new CancellationTokenSource();

            _logger.Information("Consumindo mensagens do Kafka");

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
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false
            };


            try
            {

                using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                    .SetStatisticsHandler((_, json) => _logger.Information($"Statistic: {json}"))
                    .SetLogHandler((_, log) => _logger.Information($"Log: {log}"))
                    .Build())
                {
                    consumer.Subscribe(Constants.TopicName);


                    try
                    {
                        while (true)
                        {
                            var commitMessage = true;

                            var cr = consumer.Consume(cts.Token);

                            
                            var message = cr.Message.Value;

                            var topicPartitions = new List<TopicPartition>()
                                    {
                                        { cr.TopicPartition }
                                    };

                            try
                            {
                                _logger.Information($"===========================================================");
                                
                                await ProcessMessage(message);
                                _logger.Information($"Mensagem lida: {message}. Topic/Partition: {cr.TopicPartition.Topic} / {cr.TopicPartition.Partition}");
                            }
                            catch (MyKafkaException kex)
                            {

                                _logger.Error($"Erro interno: {kex.Message}");
                                _logger.Information("Enviando para retry topic.");
                                commitMessage = await SendMessageToRetryTopic(message);
                            }
                            finally
                            {
                                if (commitMessage)
                                {
                                    consumer.StoreOffset(cr);
                                    _logger.Information("Mensagem commitada.");

                                    consumer.Pause(topicPartitions);
                                    _logger.Information($"Consumer paused. {cr.TopicPartition.Topic} / {cr.TopicPartition.Partition}");

                                    await Task.Delay(TimeSpan.FromSeconds(10));

                                    consumer.Resume(topicPartitions);
                                    _logger.Information($"Consumer resumed. {cr.TopicPartition.Topic} / {cr.TopicPartition.Partition}");

                                }
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
                _logger.Error($"Erro: {ex.Message}");
            }

        }
        private static async Task ProcessMessage(string message)
        {
            var rnd = new Random();

            var throwException = rnd.Next(1, 5) == 0;

            if (throwException)
            {
                throw new MyKafkaException($"Simulando uma exceção> Mensagem que estava sendo processada: {message}");
            }
        }

        private static async Task<bool> SendMessageToRetryTopic(string message)
        {
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = Constants.BootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var result = await producer.ProduceAsync(
                                      Constants.RetryTopicName,
                                      new Message<Null, string> { Value = message }
                                  );

                    return result.Status == PersistenceStatus.Persisted;
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"Erro ao realizar o retry. \nMensagem : {message} \nErro:{ex.Message}");
            }

            return false;

        }
    }
}
