using DS.DomainModel.Entities;
using DS.DomainModel.Factories;
using DS.Dtos.Builders;
using DS.Dtos.Exceptions;
using DS.Dtos.ResponseMessages;
using FluentAssertions;
using FluentAssertions.Execution;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using static DS.DomainModel.Entities.Route;

namespace DS.DomainModel.UnitTests.Entities
{
    public class RouteFactoryTests
    {
        private static class InvalidDataSource
        {
            public static IEnumerable<object[]> Data =>
                new List<object[]>
                {
                    new object[]
                    {
                        null,
                        ResponseMessages.Route.CreateRouteDtoRequired
                    },
                    new object[]
                    {
                        ValidDto().With(dto => dto.Start = null),
                        ResponseMessages.Validation.StartRequired
                    },
                    new object[]
                    {
                        ValidDto().With(dto => dto.Start = string.Empty),
                        ResponseMessages.Validation.StartRequired
                    },
                    new object[]
                    {
                        ValidDto().With(dto => dto.Start = "  "),
                        ResponseMessages.Validation.StartRequired
                    },
                    new object[]
                    {
                        ValidDto().With(dto => dto.End = null),
                        ResponseMessages.Validation.EndRequired
                    },
                    new object[]
                    {
                        ValidDto().With(dto => dto.End = string.Empty),
                        ResponseMessages.Validation.EndRequired
                    },
                    new object[]
                    {
                        ValidDto().With(dto => dto.End = "  "),
                        ResponseMessages.Validation.EndRequired
                    }
                };

            private static CreateRouteDtoBuilder ValidDto() => new CreateRouteDtoBuilder().WithAllValidFields();
        }

        private readonly IRouteFactory _factory;
        private CreateRouteDtoBuilder _dto;
        private Func<Route> _func;

        public RouteFactoryTests()
        {
            _factory = new RouteFactory();
        }

        [Theory]
        [MemberData(nameof(InvalidDataSource.Data), MemberType = typeof(InvalidDataSource))]
        public void Create_WhenCreatingWithInvalidDto_ThrowsDomainModelException(CreateRouteDtoBuilder dto, string expected)
        {
            GivenDto(dto);

            WhenRouteIsCreated();

            ThenExceptionShouldBeThrownWithMessage(expected);
        }

        [Fact]
        public void Create_WhenCreatingWithValidDto_ReturnsBooking()
        {
            GivenValidDto();

            WhenRouteIsCreated();

            ThenRouteShouldBeCreatedCorrectly();
        }

        #region Givens

        private void GivenDto(CreateRouteDtoBuilder dto) => _dto = dto;

        internal void GivenValidDto() => _dto = new CreateRouteDtoBuilder().WithAllValidFields();

        #endregion

        #region Whens

        private void WhenRouteIsCreated()
        {
            _func = (() => _factory.Create(_dto));
        }

        #endregion

        #region Thens

        private void ThenExceptionShouldBeThrownWithMessage(string expected)
        {
            _func
                .Should().Throw<DomainModelException>()
                .Which.ErrorMessages.Single().Should().Be(expected);
        }

        private void ThenRouteShouldBeCreatedCorrectly()
        {
            var route = _func();

            Assert.NotNull(route);

            using (new AssertionScope())
            {
                route.Start.Should().Be(_dto.Start);
                route.End.Should().Be(_dto.End);
            }
        }

        #endregion
    }
}