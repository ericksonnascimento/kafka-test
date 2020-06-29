using Serilog;

namespace Kafka.Core.Logger
{
    public class Logger
    {
        public static Serilog.Core.Logger BuildLogger()
        {
            return new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
        }
    }
}
