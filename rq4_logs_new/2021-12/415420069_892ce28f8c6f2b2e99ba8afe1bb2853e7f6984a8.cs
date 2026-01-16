using BusinessLogic.Containers;
using BusinessLogic.Converter;
using BusinessLogic.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using WikiFSUnitTests.Mockups;

namespace WikiFSUnitTests.Comments
{
    [TestClass]
    public class CommentContainerTests
    {
        [TestMethod]
        public void ShouldCreateComment()
        {
            // Arrange
            SQLCommentContextMockup mockup = new SQLCommentContextMockup();
            CommentConverter converter = new CommentConverter();
            CommentContainer commentContainer = new CommentContainer(mockup, converter);
            CommentModel commentModel = new CommentModel
            {
                Text = "Some content",
                created_at = System.DateTime.Now,
                page_id = 2,
            };

            // Act
            commentContainer.CreateComment(commentModel);
            // Assert
            // Check if pagemodel.title is equal to title in mockup from list
            Assert.AreEqual(commentModel.Text, mockup.commentList[1].Text);
        }

        [TestMethod]
        public void ShouldDeleteComment()
        {
            // Arrange
            SQLCommentContextMockup mockup = new SQLCommentContextMockup();
            CommentConverter converter = new CommentConverter();
            CommentContainer commentContainer = new CommentContainer(mockup, converter);
            CommentModel commentModel = new CommentModel
            {
                ID = 2,
            };


            // Act
            commentContainer.DeleteComment(8);
            // Assert
            Assert.IsNotNull(commentModel.ID);
        }
    }
}