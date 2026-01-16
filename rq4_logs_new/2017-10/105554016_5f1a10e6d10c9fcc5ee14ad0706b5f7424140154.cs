using Autofac;
using Common.Helpers;
using Common.Helpers.IHelpers;
using Entities;
using FileUpload.Service.Controllers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileUpload.Service
{
    class Program
    {
        static IApplicationConfig _applicationConfig = null;
        static IMessageQueueHelper _messageQueueHelper = null;
        static ICacheHelper _cacheHelper = null;

        static void Main(string[] args)
        {
            #region DI Container Builder
            var builder = new ContainerBuilder();

            builder.RegisterType<RedisHelper>().As<ICacheHelper>();
            builder.RegisterType<ApplicationConfig>().As<IApplicationConfig>();
            builder.RegisterType<RabbitMQHelper>().As<IMessageQueueHelper>();

            var container = builder.Build();
            #endregion

            using (var scope = container.BeginLifetimeScope())
            {
                #region DI Resolver
                _cacheHelper = scope.Resolve<ICacheHelper>();
                _messageQueueHelper = scope.Resolve<IMessageQueueHelper>();
                _applicationConfig = scope.Resolve<IApplicationConfig>();
                #endregion

                var fileProcessor = new FileProcessor();

                #region Processors
                //Start Message Queue Listener
                Task.Run(() => _messageQueueHelper.ReadMessages<FileMetaData>(_applicationConfig, fileProcessor.FilePushed, ""));
                //Start Message Queue Processor
                Task.Run(() => _messageQueueHelper.ReadMessages<Guid>(_applicationConfig, fileProcessor.FileDelete, ""));
                #endregion
            }

            Console.ReadLine();
        }


    }
}