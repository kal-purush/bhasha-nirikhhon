using FluentAssertions;
using Nameless.Core.UnitTests.Fixtures;

namespace Nameless.Core.UnitTests.Attributes {

    public class SingletonAttributeTests {

        [Test]
        public void GetInstance_Must_Return_Singleton_Instance_From_Singleton_Object() {
            // arrange

            // act
            var instance = SingletonAttribute.GetInstance<MySingletonClass>();

            // assert
            instance.Should().NotBeNull();
        }

        [Test]
        public void GetInstance_Must_Return_Null_From_Not_Singleton_Object() {
            // arrange

            // act
            var instance = SingletonAttribute.GetInstance<MyClass>();

            // assert
            instance.Should().BeNull();
        }

        [Test]
        public void GetInstance_Must_Return_Singleton_Instance_From_Singleton_Object_With_Different_Accessor() {
            // arrange

            // act
            var instance = SingletonAttribute.GetInstance<MySingletonClassWithDifferentAccessor>();

            // assert
            instance.Should().NotBeNull();
        }

        [Test]
        public void GetInstance_Must_Return_Null_From_Singleton_Object_Without_Attribute() {
            // arrange

            // act
            var instance = SingletonAttribute.GetInstance<MySingletonClassWithoutAttribute>();

            // assert
            instance.Should().BeNull();
        }
    }
}