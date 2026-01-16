using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace DataParser.Tests
{
    [TestFixture]
    public class HtmlDownloadTests
    {
        [Test]
        public void ParseAjaxSitesOne()
        {
            if (MakeTest($"http://www.td-dvoriki.com/toy-shop-cjg9/").Contains("Уютный коттедж"))
            {
                Assert.AreEqual(1, 1);
            }
            else
            {
                Assert.AreEqual(1, 2);
            }
        }

        [Test]
        public void ParseAjaxSitesTwo()
        {
            if (MakeTest($"http://tomik.ru/katalog/igrushki-dlja-malyshej/piramidki/")
                .Contains("Пирамидка цветная"))
            {
                Assert.AreEqual(1, 1);
            }
            else
            {
                Assert.AreEqual(1, 2);
            }
        }

        [Test]
        public void ParseWithoutAjaxSitesOne()
        {
            if (MakeTest($"http://www.rntoys.com/Categories/beads.html/")
                .Contains("Бусы геометрические цветные"))
            {
                Assert.AreEqual(1, 1);
            }
            else
            {
                Assert.AreEqual(1, 2);
            }
        }

        [Test]
        public void ParseWithoutAjaxSitesTwo()
        {
            if (MakeTest($"http://www.ural-toys.ru/brands/429/")
                .Contains("Набор кубиков 20 шт. Герои сказок"))
            {
                Assert.AreEqual(1, 1);
            }
            else
            {
                Assert.AreEqual(1, 2);
            }
        }

        [Test]
        public void ParseWithoutAjaxSitesThree()
        {
            if (MakeTest($"https://playdorado.ru/katalki/")
                .Contains("Каталка Бабочка"))
            {
                Assert.AreEqual(1, 1);
            }
            else
            {
                Assert.AreEqual(1, 2);
            }
        }

        [Test]
        public void UrlWithoutHTTP()
        {
            if (MakeTest($"www.td-dvoriki.com/toy-shop-cjg9/").Contains("Уютный коттедж"))
            {
                Assert.AreEqual(1, 1);
            }
            else
            {
                Assert.AreEqual(1, 2);
            }
        }

        [Test]
        public void UrlWithoutSlash()
        {
            if (MakeTest($"http://www.td-dvoriki.com/toy-shop-cjg9").Contains("Уютный коттедж"))
            {
                Assert.AreEqual(1, 1);
            }
            else
            {
                Assert.AreEqual(1, 2);
            }
        }


        private string MakeTest(string url)
        {
            return HtmlDownload.GetHtmlPage(url);
        }
    }
}