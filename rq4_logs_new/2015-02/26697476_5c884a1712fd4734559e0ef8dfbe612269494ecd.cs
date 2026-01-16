using System;
using System.Reflection;
using FusionMVVM.Service;
using Ploeh.AutoFixture;
using Ploeh.AutoFixture.Xunit;
using Xunit;
using Xunit.Extensions;

namespace FusionMVVM.Tests.Service.WindowLocatorTests
{
    public class ConvertNameToTypeTests
    {
        [Theory, AutoData]
        public void ConvertNameToType_WhenTypeName_IsNull(WindowLocator sut, Assembly assembly)
        {
            Assert.Throws<ArgumentNullException>(() => sut.ConvertNameToType(null, assembly));
        }

        [Theory, AutoData]
        public void ConvertNameToType_WhenAssembly_IsNull(WindowLocator sut, string typeName)
        {
            Assert.Throws<ArgumentNullException>(() => sut.ConvertNameToType(typeName, null));
        }

        [Theory]
        [InlineAutoData("", null)]
        [InlineAutoData("Ploeh.AutoFixture.Fixture", typeof(Fixture))]
        public void ConvertNameToType_ReturnsCorrectResult(string typeName, Type expected, WindowLocator sut, Assembly assembly)
        {
            var actual = sut.ConvertNameToType(typeName, assembly);
            Assert.Equal(expected, actual);
        }
    }
}