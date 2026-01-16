using BusinessLogic.Converter;
using BusinessLogic.functions;
using BusinessLogic.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using WikiFSUnitTests.Mockups;

namespace WikiFSUnitTests.Pages
{
    [TestClass]
    public class PageTests
    {
        [TestMethod]
        public void ShouldDeletePage()
        {
            // Arrange
            SQLPageContextMockup mockup = new SQLPageContextMockup();
            PageConverter converter = new PageConverter();
            Page page = new Page(mockup, converter);
            PageModel pageModel = new PageModel
            {
                ID = 2,
            };


            // Act
            page.Delete(8);
            // Assert
            Assert.IsNotNull(pageModel.ID);
        }

        [TestMethod]
        public void ShouldEditPage()
        {
            // Arrange
            SQLPageContextMockup mockup = new SQLPageContextMockup();
            PageConverter converter = new PageConverter();
            Page page = new Page(mockup, converter);
            PageModel pageModel = new PageModel
            {
                ID = 1,
                Title = "Edited Title",
                Text = "Some content bruh",
                updated_at = DateTime.Now,
            };

            // Act
            page.Update(pageModel);
            // Assert
            Assert.AreEqual(pageModel.Title, mockup.pageList[0].Title);
        }
    }
}