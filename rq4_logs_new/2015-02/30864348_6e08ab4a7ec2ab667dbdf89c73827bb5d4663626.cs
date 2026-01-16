using System;
using Xunit;

namespace Hal.WebApi.Formatter.UnitTest
{
    public class HalLinkTest
    {
        [Fact]
        public void Href_OnConstructionWithNullHrefParamInput_ThrowsArgumentNullException()
        {
            // Arrange
            string href = null;

            // Act
            Action construction = () => new HalLink(href);

            // Assert
            var exception = Assert.Throws<ArgumentNullException>(() => construction());
            Assert.Equal("href", exception.ParamName);
        }

        [Fact]
        public void Href_OnConstructionWithEmptyHrefParamInput_ThrowsArgumentException()
        {
            // Arrange
            string href = string.Empty;

            // Act
            Action construction = () => new HalLink(href);

            // Assert
            var exception = Assert.Throws<ArgumentException>(() => construction());
            Assert.Equal("href", exception.ParamName);
        }

        [Fact]
        public void Href_OnConstructionWithValidHrefParamInput_IsSetSuccessfully()
        {
            // Arrange
            string href = "http://sampleurl.com";

            // Act
            var link = new HalLink(href);

            // Assert            
            Assert.Equal(href, link.Href);
        }

        [Fact]
        public void Templated_OnConstructionWithTemplatedHref_DefaultsIsTemplatedToTrue()
        {
            // Arrange
            string href = "http://sampleurl.com/{something}";

            // Act 
            var link = new HalLink(href);

            // Assert
            Assert.True(link.Templated);
        }

        [Fact]
        public void Templated_OnConstructionWithNormalUrl_DefaultsToFalse()
        {
            // Arrange
            string href = "http://sampleurl.com";

            // Act
            var link = new HalLink(href);

            // Assert
            Assert.False(link.Templated);
        }

        [Fact]
        public void Templated_OnOverloadedConstruction_IsSetSuccessfully()
        {
            // Arrange
            string href = "http://sampleurl.com/{0}";
            bool templated = true;

            // Act
            var link = new HalLink(href, templated);

            // Assert
            Assert.True(link.Templated);
        }

        [Fact]
        public void Type_OnConstruction_DefaultsToNull()
        {
            // Arrange
            string href = "http://sampleurl.com";

            // Act
            var link = new HalLink(href);

            // Assert
            Assert.Null(link.Type);
        }

        [Fact]
        public void Type_OnOverloadedConstruction_IsSetSuccessfully()
        {
            // Arrange
            string href = "http://sampleurl.com/{0}";
            string type = "application/vnd.hal+json";

            // Act
            var link = new HalLink(href, type: type);

            // Assert
            Assert.Equal(type, link.Type);
        }

        [Fact]
        public void Depreciation_OnConstruction_DefaultsToNull()
        {
            // Arrange
            string href = "http://sampleurl.com";

            // Act
            var link = new HalLink(href);

            // Assert
            Assert.Null(link.Depreciation);
        }

        [Fact]
        public void Depreciation_OnOverloadedConstruction_IsSetSuccessfully()
        {
            // Arrange
            string href = "http://sampleurl.com/{0}";
            string depreciation = "http://someurl";

            // Act
            var link = new HalLink(href, depreciation: depreciation);

            // Assert
            Assert.Equal(depreciation, link.Depreciation);
        }

        [Fact]
        public void Name_OnConstruction_DefaultsToNull()
        {
            // Arrange
            string href = "http://sampleurl.com";

            // Act
            var link = new HalLink(href);

            // Assert
            Assert.Null(link.Name);
        }

        [Fact]
        public void Name_OnOverloadedConstruction_IsSetSuccessfully()
        {
            // Arrange
            string href = "http://sampleurl.com/{0}";
            string name = "Some link Name";

            // Act
            var link = new HalLink(href, name: name);

            // Assert
            Assert.Equal(name, link.Name);
        }

        [Fact]
        public void Profile_OnConstruction_DefaultsToNull()
        {
            // Arrange
            string href = "http://sampleurl.com";

            // Act
            var link = new HalLink(href);

            // Assert
            Assert.Null(link.Profile);
        }

        [Fact]
        public void Profile_OnOverloadedConstruction_IsSetSuccessfully()
        {
            // Arrange
            string href = "http://sampleurl.com/{0}";
            string profile = "Profile";

            // Act
            var link = new HalLink(href, profile: profile);

            // Assert
            Assert.Equal(profile, link.Profile);
        }

        [Fact]
        public void Title_OnConstruction_DefaultsToNull()
        {
            // Arrange
            string href = "http://sampleurl.com";

            // Act
            var link = new HalLink(href);

            // Assert
            Assert.Null(link.Title);
        }

        [Fact]
        public void Title_OnOverloadedConstruction_IsSetSuccessfully()
        {
            // Arrange
            string href = "http://sampleurl.com/{0}";
            string title = "Title";

            // Act
            var link = new HalLink(href, title: title);

            // Assert
            Assert.Equal(title, link.Title);
        }

        [Fact]
        public void HrefLang_OnConstruction_DefaultsToNull()
        {
            // Arrange
            string href = "http://sampleurl.com";

            // Act
            var link = new HalLink(href);

            // Assert
            Assert.Null(link.HrefLang);
        }

        [Fact]
        public void HrefLang_OnOverloadedConstruction_IsSetSuccessfully()
        {
            // Arrange
            string href = "http://sampleurl.com/{0}";
            string hrefLang = "HrefLang";

            // Act
            var link = new HalLink(href, hrefLang: hrefLang);

            // Assert
            Assert.Equal(hrefLang, link.HrefLang);
        }
    }

    public class HalLinkCurieTest
    {
        [Fact]
        public void Name_OnConstructionWithNullNameParamInput_ThrowsArgumentNullException()
        {
            // Arrange
            string href = "www.google.com";
            string name = null;

            // Act
            Action construction = () => new HalLinkCurie(href, name);

            // Assert
            var exception = Assert.Throws<ArgumentNullException>(() => construction());
            Assert.Equal("name", exception.ParamName);
        }

        [Fact]
        public void Name_OnConstructionWithEmptyNameParamInput_ThrowsArgumentException()
        {
            // Arrange
            string href = "www.google.com";
            string name = string.Empty;

            // Act
            Action construction = () => new HalLinkCurie(href, name);

            // Assert
            var exception = Assert.Throws<ArgumentException>(() => construction());
            Assert.Equal("name", exception.ParamName);
        }
    }
}