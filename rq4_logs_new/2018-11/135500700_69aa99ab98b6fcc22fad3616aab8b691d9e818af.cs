using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Routing;
using Microsoft.AspNetCore.Mvc.ViewComponents;
using Moq;
using ToDoApp.Models;
using ToDoApp.ViewComponents;
using Xunit;

namespace ToDoApp.Test
{
    public class CardViewComponent
    {
        [Fact]
        public void CardGroup_ShouldGetRequestedCountForView()
        {
            // Arrange
            var viewComponent = new CardGroupViewComponent();
            var urlHelper = new Mock<IUrlHelper>();
            urlHelper.Setup(o => o.Action(It.IsAny<UrlActionContext>())).Returns("action/controller");
            viewComponent.Url = urlHelper.Object;

            //Act
            var componentResult = viewComponent.Invoke(3) as ViewViewComponentResult;
            var model = componentResult.ViewData.Model as IEnumerable<CardViewModel>;

            //Assert
            Assert.Equal(3, model.Count());
        }

        [Fact]
        public void CardGroup_ShouldOrderCardsOldestFirstForView()
        {
            // Arrange
            var regex = new Regex("Created (.*) days ago");
            var viewComponent = new CardGroupViewComponent();
            var urlHelper = new Mock<IUrlHelper>();
            urlHelper.Setup(o => o.Action(It.IsAny<UrlActionContext>())).Returns("action/controller");
            viewComponent.Url = urlHelper.Object;

            //Act
            var componentResult = viewComponent.Invoke(3) as ViewViewComponentResult;
            var model = (componentResult.ViewData.Model as IEnumerable<CardViewModel>).ToList();

            //Assert
            for (int i = 0; i < model.Count; i++)
            {
                var card = model[i];
                var cardMatch = regex.Match(card.ActionDescription).Groups.Skip(1).First().Value;
                var cardDay = int.Parse(cardMatch);
                foreach (var item in model.Skip(i))
                {
                    var itemMatch = regex.Match(item.ActionDescription).Groups.Skip(1).First().Value;
                    var itemDay = int.Parse(itemMatch);
                    Assert.True(cardDay >= itemDay);
                }
            }
        }
    }
}