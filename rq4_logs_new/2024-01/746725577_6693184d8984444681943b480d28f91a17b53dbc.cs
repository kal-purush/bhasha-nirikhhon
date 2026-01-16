using globalmsgAPI.Controllers;
using globalmsgAPI.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Moq;
using System.Threading.Tasks;
using Xunit;


namespace globalmsgAPI.Tests;

public class MessageControllerTests
{
    [Fact]
    public async void PostMessage_ShouldCreateNewItem()
    {
        var mockOptions = new DbContextOptionsBuilder<MessageContext>().Options;
        var mockMessageContext = new Mock<MessageContext>(mockOptions);
        var controller = new MessageController(mockMessageContext.Object);

        var result = await controller.PostMessage(new Message { ID = 1, Content = "Hola, este es un mensaje de prueba.", User = "Usuario123" });

        var createdAtActionResult = Assert.IsType<CreatedAtActionResult>(result.Result);
        Assert.NotNull(createdAtActionResult);

    }
}