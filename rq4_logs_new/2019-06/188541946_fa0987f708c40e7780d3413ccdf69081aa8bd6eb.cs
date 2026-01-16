using System.IO;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Extensions;
using Xunit;

namespace PingDong.Http
{
    public class NullToDefaultConverterTests
    {
        [Fact]
        public void CanConvert_ShouldBeFalse_When_TypeMismatch()
        {
            var convert = new NullToDefaultConverter<int>();

            Assert.False(convert.CanConvert(typeof(string)));
        }

        [Fact]
        public void CanWrite_ShouldBeTrue()
        {
            var convert = new NullToDefaultConverter<int>();

            Assert.True(convert.CanWrite);
        }

        [Fact]
        public void ReadJson_ShouldReturnDefault_WhenNull()
        {
            var convert = new NullToDefaultConverter<int>();

            var json = @"{
                           'CPU': 'Intel',
                           'PSU': '500W',
                        }";

            using (var stringReader = new StringReader(json))
            using (var reader = new JsonTextReader(stringReader))
            {
                var pc = convert.ReadJson(reader, typeof(Computer), null, null);

                Assert.Equal(default, ((Computer) pc).Memory);
            }
        }

        [Fact]
        public void WriteJson_ShouldSaveDefault_WhenDefault()
        {
            var convert = new NullToDefaultConverter<int>();
            
            var sb = new StringBuilder();
            var sw = new StringWriter(sb);

            using (var writer = new JsonTextWriter(sw))
            {
                convert.WriteJson(writer, default(int), null);

                var txt = sb.ToString();

                Assert.Equal(default(int).ToString(), txt);
            }
        }

        [Fact]
        public void WriteJson_ShouldSaveDefault_WhenNull()
        {
            var convert = new NullToDefaultConverter<int>();
            
            var sb = new StringBuilder();
            var sw = new StringWriter(sb);

            using (var writer = new JsonTextWriter(sw))
            {
                convert.WriteJson(writer, null, null);

                var txt = sb.ToString();

                Assert.Equal("null", txt);
            }
        }

        [Fact]
        public void WriteJson_ShouldSave_WhenNotNull()
        {
            var convert = new NullToDefaultConverter<int>();
            
            var sb = new StringBuilder();
            var sw = new StringWriter(sb);

            using (var writer = new JsonTextWriter(sw))
            {
                convert.WriteJson(writer, 10, null);

                var txt = sb.ToString();

                Assert.Equal("10", txt);
            }
        }

        internal class Computer
        {
            public string Cpu { get; set; }
            public string Psu { get; set; }
            public int Memory { get; set; }
        }
    }
}