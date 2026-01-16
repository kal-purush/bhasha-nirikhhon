using Domain;
using NUnit.Framework;


namespace UnitTests
{
using NUnit.Framework;

namespace UnitTests
{
    public class ArticleTests
    {
        [Test]
        public void TestConstructor()
        {
            // Arrange
            var id = 1;
            var name = "Article 1";
            var urlImage = "http://example.com/image.jpg";
            var publicationDate = new DateTime(2022, 1, 1);
            var categoryName = "News";
            var description = "This is a test article.";
            var idUser = 123;

            // Act
            var article = new Article
            {
                Id = id,
                Name = name,
                URLImage = urlImage,
                PublicationDate = publicationDate,
                CategoryName = categoryName,
                description = description,
                IdUser = idUser
            };

            // Assert
            Assert.AreEqual(id, article.Id);
            Assert.AreEqual(name, article.Name);
            Assert.AreEqual(urlImage, article.URLImage);
            Assert.AreEqual(publicationDate, article.PublicationDate);
            Assert.AreEqual(categoryName, article.CategoryName);
            Assert.AreEqual(description, article.description);
            Assert.AreEqual(idUser, article.IdUser);
        }

        
        [Test]
        public void TestIsInCategory()
        {
            // Arrange
            var newsArticle = new Article { CategoryName = "News" };
            var sportsArticle = new Article { CategoryName = "Sports" };

            // Act and Assert
            Assert.IsTrue(newsArticle.IsInCategory("News"));
            Assert.IsFalse(newsArticle.IsInCategory("Sports"));
            Assert.IsTrue(sportsArticle.IsInCategory("Sports"));
            Assert.IsFalse(sportsArticle.IsInCategory("News"));
        }
    }
}

}