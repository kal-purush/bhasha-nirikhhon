using System;
using System.Linq;

using AutoFixture;

using FluentAssertions;

using Moq;

using NUnit.Framework;

using Ralfred.Common.DependencyInjection;
using Ralfred.Common.Helpers.Serialization;
using Ralfred.Common.Types;
using Ralfred.SecretsProvider.Services;
using Ralfred.SecretsProvider.Services.Formatters;


namespace SecretsProvider.UnitTests.Services
{
	[TestFixture]
	public class FormatterResolverTests
	{
		[SetUp]
		public void Setup()
		{
			_fixture = new Fixture();

			_serializerResolver = new Mock<StorageResolvingExtensions.SerializerResolver>();

			_target = new FormatterResolver(_serializerResolver.Object);
		}

		[Test]
		public void ResolveTest()
		{
			// arrange
			_serializer = new Mock<ISerializer>();

			var type = _fixture.Create<Generator<FormatType>>().FirstOrDefault(x => new[]
			{
				FormatType.Json,
				FormatType.Xml
			}.Contains(x));

			_serializerResolver.Setup(x => x(type)).Returns(_serializer.Object);

			// act
			var result = _target.Resolve(type);

			// assert
			new[] { typeof(JsonContentFormatter), typeof(XmlContentFormatter) }.Contains(result.GetType()).Should().BeTrue();
		}

		[Test]
		public void Resolve_KeyValueTest()
		{
			// arrange
			_serializer = new Mock<ISerializer>();

			var type = _fixture.Create<Generator<FormatType>>().FirstOrDefault(x => x == FormatType.Env);

			_serializerResolver.Setup(x => x(type)).Returns(_serializer.Object);

			// act
			var result = _target.Resolve(type);

			// assert
			result.GetType().Should().Be(typeof(KeyValueContentFormatter));
		}

		[Test]
		public void Resolve_UndefinedTypeTest()
		{
			// arrange
			_serializer = new Mock<ISerializer>();

			var type = _fixture.Create<Generator<FormatType>>().FirstOrDefault(x => !new[]
			{
				FormatType.Json,
				FormatType.Xml,
				FormatType.Env
			}.Contains(x));

			_serializerResolver.Setup(x => x(type)).Returns(_serializer.Object);

			// act
			Assert.Throws<ArgumentOutOfRangeException>(() => _target.Resolve(type));
		}

		private IFixture _fixture;

		private Mock<ISerializer> _serializer;
		private Mock<StorageResolvingExtensions.SerializerResolver> _serializerResolver;

		private FormatterResolver _target;
	}
}