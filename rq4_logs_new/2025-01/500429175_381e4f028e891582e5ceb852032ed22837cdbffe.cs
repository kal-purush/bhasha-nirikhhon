using System.Net;
using Crypto.API.Controllers;
using Crypto.Application.Modules.Crypto.Commands.UpdateInfo;
using Crypto.IntegrationTests.Common;
using Crypto.Shared.Builders;
using Crypto.Shared.Utilities;
using FluentAssertions;
using Tests.Common.Extensions;
using Tests.Common.Interfaces.Claims.Models;

namespace Crypto.IntegrationTests.CryptoController;

public class FetchSingleEndpointTests(CryptoApiFactory factory)  : BaseCryptoEndpointTests(factory)
{
    [Fact]
    public async Task ShouldReturnNotFound_WhenNonExistentSymbolProvided()
    {
        //Arrange
        Client.WithRole(UserRole.Admin);
        var symbol = SymbolGenerator.Generate();
    
        //Act
        var response = await Client.GetAsync(EndpointsConfigurations.CryptoEndpoints.BuildSingleUrl(symbol));

        //Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }
    
    [Fact]
    public async Task ShouldReturnOk_WhenExistentSymbolProvided()
    {
        //Arrange
        Client.WithRole(UserRole.Admin);
        var cryptoEntity = await DataManager.Add(new CryptoEntityBuilder().Build());
        await DataManager.AddPrice(new CryptoPriceEntityBuilder()
            .WithCryptoItemId(cryptoEntity.Id)
            .Build());
        
        //Act
        var response = await Client.GetAsync(EndpointsConfigurations.CryptoEndpoints.BuildSingleUrl(cryptoEntity.Symbol));

        //Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}