using Autocomplete;
using Autocomplete.Business.Interface;
using Autocomplete.Business.Repository;
using Autocomplete.Data.Contexto;
using Autocomplete.Test.Config;
using System;
using Xunit;

namespace Autocompletetest
{
    public class CityTests
    {
        private readonly IntegrationTestsFixture<Startup> _fixture;
        public CityTests(IntegrationTestsFixture<Startup> fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public void CityRepository_ObterEnderecoPorCidadeAsync_ResultCityEmptyWhenNameHasNoValue()
        {
            //Arrange
            var inicialResponse = _fixture.Client.GetAsync("/");
            //Act

            //Assert

        }
    }
}