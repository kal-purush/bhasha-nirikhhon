using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Nexus.Link.Libraries.Core.Platform.Configurations;

namespace Nexus.Link.Libraries.Web.Tests.ServiceAuthentication
{
    public class MockLeverConfiguration : ILeverConfiguration
    {
        /// <summary>
        /// This is where all the configuration values are stored
        /// </summary>
        public JObject JObject { get; internal set; }

        public MockLeverConfiguration(JObject jObject)
        {
            JObject = jObject;
        }

        /// <inheritdoc />
        public T Value<T>(object key)
        {
            return JObject.Value<T>(key);
        }

        /// <inheritdoc />
        public T MandatoryValue<T>(object key)
        {
            var value = Value<T>(key);
            return value;
        }
    }
}