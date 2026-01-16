using System;
using Dolittle.Logging;
using Moq;

namespace Dolittle.Runtime.Events.Specs.MongoDB
{
    public class given
    {
        public static ILogger a_logger()
        {
            var logger_mock = new Mock<ILogger>();
            logger_mock.Setup(l => l.Error(Moq.It.IsAny<Exception>(), Moq.It.IsAny<string>(), Moq.It.IsAny<string>(), Moq.It.IsAny<int>(),Moq.It.IsAny<string>()))
                .Callback<Exception,string,string,int,string>((ex,msg,fp,ln,m) => Console.WriteLine(ex.ToString()));
            logger_mock.Setup(l => l.Debug(Moq.It.IsAny<string>(),Moq.It.IsAny<string>(),Moq.It.IsAny<int>(),Moq.It.IsAny<string>()))
                .Callback<string,string,int,string>((msg,fp,ln,m) => Console.WriteLine(msg));
            return logger_mock.Object;
        }   
    }
}