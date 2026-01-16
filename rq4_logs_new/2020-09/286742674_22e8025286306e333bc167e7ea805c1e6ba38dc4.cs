using FluentAssertions;
using NUnit.Framework;
using SFA.DAS.Tools.Servicebus.Support.Infrastructure.Services;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SFA.DAS.Tools.Servicebus.Support.Infrastructure.UnitTests.Services.MessageDetail
{
    public class WhenMessageContainsSensitiveInformation
    {
        [Test]
        public async Task ThenSensitiveInformationIsRedacted()
        {
            var tokens = new List<string>()
            {
                "(.*SharedAccessKey=)([A-Za-z0-9]+=)(.*)"
            };
            var values = new List<KeyValuePair<string, object>>();
            values.Add(new KeyValuePair<string, object>("Key1", "NotSensitive"));
            values.Add(new KeyValuePair<string, object>("Key2", "xyz;SharedAccessKey=12345qwerty="));

            var redactor = new MessageDetailRedactor(tokens);

            var result = redactor.Redact(values);

            result.Should().NotBeNull();
            result.Count(x => x.Value == "[REDACTED]").Should().Be(1);
            result.Count(x => x.Value == "NotSensitive").Should().Be(1);
        }
    }
}