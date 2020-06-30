using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Core.Constants;
using Kafka.Core.Exceptions;
using Kafka.Core.Model;
using Newtonsoft.Json;

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

                            var message = JsonConvert.DeserializeObject<EmailMessage>(cr.Message.Value);

                            var topicPartitions = new List<TopicPartition>()
                                    {
                                        { cr.TopicPartition }
                                    };

                            try
                            {
                                var now = DateTime.Now;
                                
                                _logger.Information($"===========================================================");
                                _logger.Information($"Hora atual: {now}");

                                if (DateTime.Compare(now, message.Timestamp) == -1)
                                {
                                    var waitTime = ((TimeSpan)(message.Timestamp - now)).TotalMilliseconds;

                                    _logger.Warning($"Hora que deve processar: {message.Timestamp}. Esperar: {waitTime} ms");

                                    consumer.Pause(topicPartitions);

                                    await Task.Delay(TimeSpan.FromMilliseconds(waitTime));

                                    consumer.Resume(topicPartitions);

                                    //commitMessage = await SendMessageToRetryTopic(message);
                                }

                                await ProcessMessage(message);
                            }
                            catch (MyKafkaException kex)
                            {

                                _logger.Error($"Erro interno: {kex.Message}");
                                _logger.Information("Enviando para retry topic.");
                                commitMessage = await SendMessageToRetryTopic(message, true);
                            }
                            finally
                            {
                                if (commitMessage)
                                {
                                    consumer.StoreOffset(cr);
                                    _logger.Information($"Mensagem '{message.Id}' commitada.");

                                    //consumer.Pause(topicPartitions);
                                    //_logger.Information($"Consumer paused. {cr.TopicPartition.Topic} / {cr.TopicPartition.Partition}");

                                    //await Task.Delay(TimeSpan.FromSeconds(10));

                                    //consumer.Resume(topicPartitions);
                                    //_logger.Information($"Consumer resumed. {cr.TopicPartition.Topic} / {cr.TopicPartition.Partition}");

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
        private static async Task ProcessMessage(EmailMessage message)
        {
            var rnd = new Random();

            var throwException = rnd.Next(1, 6) == 2;
            //var throwException = rnd.Next(1, 6) % 2 == 0;

            _logger.Information($"Mensagem : {message.ToString()}");

            if (throwException)
            {
                throw new MyKafkaException($"Simulando uma exceção> Mensagem que estava sendo processada: {message.ToString()}");
            }
        }

        private static async Task<bool> SendMessageToRetryTopic(EmailMessage message, bool addTime = false)
        {
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = Constants.BootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    if (addTime)
                    {
                        message.Timestamp = message.Timestamp.AddSeconds(20);
                        message.Attempts += 1;
                    }

                    var result = await producer.ProduceAsync(
                                      Constants.TopicName,
                                      new Message<Null, string> { Value = JsonConvert.SerializeObject(message) }
                                  );

                    return result.Status == PersistenceStatus.Persisted;
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"Erro ao realizar o retry. \nMensagem : {message.ToString()} \nErro:{ex.Message}");
            }

            return false;

        }
    }
}
