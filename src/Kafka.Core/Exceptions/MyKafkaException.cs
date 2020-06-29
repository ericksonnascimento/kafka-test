using System;

namespace Kafka.Core.Exceptions
{
    public class MyKafkaException : Exception
    {
        public MyKafkaException(string message) : base(message)
        {

        }
    }
}
