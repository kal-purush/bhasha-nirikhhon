using Moq;
using NUnit.Framework;
using ServerApp.BLL.Services.Base;
using ServerApp.DAL.Infrastructure;
using System;

namespace ServerApp.BLL.Test.BaseServiceTests
{
    [TestFixture]
    public class BaseServiceTests
    {
        private BaseService<SomeModel> _service;

        [SetUp]
        public void Setup()
        {
            var unitOfWorkMock = new Mock<IUnitOfWork>();
            _service = new BaseService<SomeModel>(unitOfWorkMock.Object);
        }

        // Kiểm tra trường hợp thuộc tính hợp lệ
        [Test]
        public void ValidateModelPropertiesWithAttribute_ValidProperties_DoesNotThrowException()
        {
            // Arrange
            var model = new SomeModel
            {
                Property1 = "Valid String",
                Property2 = 123
            };

            // Act & Assert
            Assert.DoesNotThrow(() => _service.ValidateModelPropertiesWithAttribute(model));
        }

        // Kiểm tra trường hợp thuộc tính là null
        [Test]
        public void ValidateModelPropertiesWithAttribute_PropertyIsNull_ThrowsArgumentException()
        {
            // Arrange
            var model = new SomeModel
            {
                Property1 = null, // Property1 là null
                Property2 = 123
            };

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _service.ValidateModelPropertiesWithAttribute(model));
            Assert.That(ex.Message, Does.Contain("Property 'Property1' in model 'SomeModel' cannot be null."));
        }

        // Kiểm tra trường hợp thuộc tính là chuỗi rỗng
        [Test]
        public void ValidateModelPropertiesWithAttribute_PropertyIsEmptyString_ThrowsArgumentException()
        {
            // Arrange
            var model = new SomeModel
            {
                Property1 = "", // Property1 là chuỗi rỗng
                Property2 = 123
            };

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _service.ValidateModelPropertiesWithAttribute(model));
            Assert.That(ex.Message, Does.Contain("Property 'Property1' in model 'SomeModel' cannot be an empty string or whitespace."));
        }

        // Kiểm tra trường hợp thuộc tính chứa chỉ khoảng trắng
        [Test]
        public void ValidateModelPropertiesWithAttribute_PropertyIsWhitespace_ThrowsArgumentException()
        {
            // Arrange
            var model = new SomeModel
            {
                Property1 = "   ", // Property1 chỉ chứa khoảng trắng
                Property2 = 123
            };

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _service.ValidateModelPropertiesWithAttribute(model));
            Assert.That(ex.Message, Does.Contain("Property 'Property1' in model 'SomeModel' cannot be an empty string or whitespace."));
        }

        [Test]
        public void ValidateModelPropertiesWithAttribute_PropertyHasSkipValidationAttribute_DoesNotThrowException()
        {
            // Arrange
            var model = new SomeModelWithSkipValidation
            {
                Property1 = "fsdf",
                Property2 = 123
            };

            // Act & Assert
            // Kiểm tra không có exception khi gọi ValidateModelPropertiesWithAttribute
            Assert.DoesNotThrow(() => _service.ValidateModelPropertiesWithAttribute(model));
        }
    }

    // Mô hình test
    public class SomeModel
    {
        [SkipValidation]
        public string? Property1 { get; set; }
        public int Property2 { get; set; }
    }

    // Mô hình với SkipValidationAttribute
    public class SomeModelWithSkipValidation
    {
        [SkipValidation]
        public string Property1 { get; set; }
        public int Property2 { get; set; }
    }

    // Attribute SkipValidation
    [AttributeUsage(AttributeTargets.Property)]
    public class SkipValidationAttribute : Attribute
    {
    }
}