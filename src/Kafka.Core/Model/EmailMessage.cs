using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Core.Model
{
    public class EmailMessage
    {
        public Guid Id { get; set; }
        public DateTime Timestamp { get; set; }
        public string Message { get; set; }
        public int Attempts { get; set; }

        public EmailMessage()
        {
            Id = Guid.NewGuid();
            Timestamp = DateTime.Now;
            Attempts = 0;
        }

        public override string ToString()
        {
            return $"Id: {Id}. Timestamp: {Timestamp}. Attempts: {Attempts}";
        }
    }
}
