using Autofac;
using RJL.UIP.HW8.EarthAreaCalculator.BAL.Services;
using RJL.UIP.HW8.EarthAreaCalculator.DAL.Services;
using RJL.UIP.HW8.EarthAreaCalculator.Shared.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RJL.UIP.EarthAreaCalculator.Core.DI
{
    public class AppContainer
    {
        private static IContainer _container;

        public static void ConfigureContainer()
        {
            var builder = new ContainerBuilder();

            builder.RegisterType<FileStorage>().As<IFileStorage>().SingleInstance();
            builder.RegisterType<ConsoleStorage>().As<IConsoleStorage>().SingleInstance();
            builder.RegisterType<StorageManager>().As<IStorageManager>().SingleInstance();
            builder.RegisterType<PointsValidator>().As<IPointsValidator>();
            builder.RegisterType<AreaCalculator>().As<IAreaCalculator>().SingleInstance();
            builder.RegisterType<LogHandler>().As<ILogHandler>().SingleInstance();
            builder.RegisterType<Logger>().As<ILogger>().SingleInstance();

            _container = builder.Build();
        }

        public static object Resolve(Type typeName)
        {
            return _container.Resolve(typeName);
        }

        public static T Resolve<T>()
        {
            return _container.Resolve<T>();
        }
    }
}