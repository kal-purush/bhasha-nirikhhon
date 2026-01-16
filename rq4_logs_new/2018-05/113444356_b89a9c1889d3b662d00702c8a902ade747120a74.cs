using System;
using Autofac;
using Newtonsoft.Json.Linq;

namespace Lykke.Sdk
{
    public static class AutofacExtensions
    {
        public static void RegisterSettings<TSettings>(this ContainerBuilder builder, string jsonPath)
        {
            builder.Register(ctx =>
            {
                var settings = ctx.Resolve<JObject>();

                if (settings == null)
                    throw new ApplicationException("General json settings must be provided");

                var node = settings.SelectToken(jsonPath);

                if (node == null)
                    throw new ArgumentException("jsonPath is invalid");

                return node.ToObject<TSettings>();
            }).SingleInstance();
        }
    }
}