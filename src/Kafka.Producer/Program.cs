using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Core.Constants;
using Kafka.Core.Model;
using Newtonsoft.Json;

namespace Kafka.Producer
{
    class Program
    {
        private static Serilog.Core.Logger _logger;

        static async Task Main(string[] args)
        {
            _logger = Core.Logger.Logger.BuildLogger();

            _logger.Information("Envio de mensagens com o Kafka");

            try
            {
                //await CreateTopicsAndPartitions();

                if (int.TryParse(args.Length > 0 ? args[0] : "1", out int quantity))
                {
                    await SendMessages(quantity);
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"Erro principal: {ex.Message}");
            }
        }

        private static async Task CreateTopicsAndPartitions()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = Constants.BootstrapServers
            };

            using (var admin = new AdminClientBuilder(config).Build())
            {
                await admin.CreateTopicsAsync(new TopicSpecification[] {
                        new TopicSpecification { Name = Constants.TopicName, NumPartitions = 2, ReplicationFactor = 1 }
                    });
            }
        }

        private static async Task SendMessages(int quantity)
        {
            var rnd = new Random();

            var config = new ProducerConfig
            {
                BootstrapServers = Constants.BootstrapServers
            };

            using (var producer = new ProducerBuilder<Null, string>(config)
                .SetErrorHandler((_, error) => _logger.Error($"Erro: {error}"))
                .Build())
            {
                for (int i = 0; i < quantity; i++)
                {
                    var mailMessage = new EmailMessage { Message = $"Mensagem de teste: {rnd.Next()}", Timestamp = DateTime.Now.AddSeconds(10) };

                    var result = await producer.ProduceAsync(
                                      Constants.TopicName,
                                      new Message<Null, string> { Value = JsonConvert.SerializeObject(mailMessage) }
                                  );

                    //var result = await producer.ProduceAsync
                    //    (new TopicPartition(Constants.TopicName, new Partition(i)),
                    //    new Message<Null, string> { Value = message });


                    _logger.Information($"Mensagem: {mailMessage.ToString()} - Status: {result.Status.ToString()} / Topic/Partition: {result.TopicPartition.Topic}/{result.TopicPartition.Partition} ");
                }

                producer.Flush(TimeSpan.FromSeconds(6));
                _logger.Information($" {quantity} mensagens produzidas.");
            }
        }
    }
}
