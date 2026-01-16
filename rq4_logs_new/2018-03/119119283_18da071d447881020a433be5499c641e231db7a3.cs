using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CreativeCravings.Controllers;
using System.Web.Mvc;
using CreativeCravings.Models;
using Moq;
using System.Linq;

namespace CreativeCravings.Tests.Controllers
{

    [TestClass]
    public class PostsControllerTest
    {
        [TestMethod]
        public void Index_Returns_ActionResult()
        {
            // Arrange
            PostsController controller = new PostsController();

            // Act
            ActionResult result = controller.Index() as ActionResult;

            // Assert
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void Index_Contains_ListOfPosts_Model()
        {
            // Arrange
            Mock<IPostRepository> postMock = new Mock<IPostRepository>();

            postMock.Setup(m => m.Posts).Returns(new Post[]
            {
                new Post {ID=1, Title="Test post 1",Body="This is the body for test 1", AuthorID="1"},
                new Post {ID=2, Title="Test post 2",Body="This is the body for test 2", AuthorID="1"},
                new Post {ID=3, Title="Test post 3",Body="This is the body for test 3", AuthorID="1"}
            }.AsQueryable());

            PostsController controller = new PostsController(postMock.Object);

            // Act
            var actual = (List<Post>)controller.Index().Model;

            // Assert
            Assert.IsInstanceOfType(actual, typeof(List<Post>));
        }

    }
}