using Catcheap.API.Controllers.MiscControl;
using Catcheap.API.Models.MiscModels;
using Catcheap.API.Services.NordpoolPriceServices;
using Catcheap.Tests.CatcheapApi.MockRepo;
using Microsoft.AspNetCore.Mvc;


namespace Catcheap.Tests.CatcheapApi.ControllerTests
{
    public class NordpoolPriceControllerTest
    {
        [Fact]
        public void GetNordpoolPricesTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            var respons = (OkObjectResult)controller.GetNordpoolPrices();
            var rez = respons.Value;

            List<NordpoolPrice> l = (List<NordpoolPrice>)rez;
            Assert.Equal(69, l[0].Id);

        }

        [Fact]
        public void GetNordpoolPriceTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            var respons = (OkObjectResult)controller.GetNordpoolPrice(69);
            var rez = respons.Value;

            NordpoolPrice l = (NordpoolPrice)rez;
            Assert.Equal(69, l.Id);

        }

        [Fact]
        public void GetNordpoolPriceByDateTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            var respons = (OkObjectResult)controller.GetNordpoolPriceByDate(new DateTime(2022,12,21,12,0,0));
            var rez = respons.Value;

            NordpoolPrice l = (NordpoolPrice)rez;
            Assert.Equal(1, l.Price);

        }

        [Fact]
        public void CreateNordpoolPriceTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            NordpoolPrice price = new NordpoolPrice();
            price.DateAndTime = new DateTime(2022, 12, 21, 12, 0, 0);
            var respons = (OkObjectResult)controller.CreateNordpoolPrice(price);

            Assert.Equal(200, respons.StatusCode);

        }

        [Fact]
        public void UpdateNordpoolPriceTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            NordpoolPrice price = new NordpoolPrice();
            price.Id = 69;
            price.DateAndTime = new DateTime(2022, 12, 21, 12, 0, 0);
            var respons = (NoContentResult)controller.UpdateNordpoolPrice(69,price);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void DeleteNordpoolPriceTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            var respons = (NoContentResult)controller.DeleteNordpoolPrice(69);

            Assert.Equal(204, respons.StatusCode);

        }

        [Fact]
        public void GetNordpoolPriceCheapestTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            var respons = (OkObjectResult)controller.GetNordpoolPriceCheapest(new DateTime(1999,12,12,12,0,0));
            var rez = respons.Value;

            Assert.Equal(69, ((NordpoolPrice)rez).Id);

        }

        [Fact]
        public void GetNordpoolPriceMostExpensiveTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            var respons = (OkObjectResult)controller.GetNordpoolPriceMostExpensive(new DateTime(1999, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            Assert.Equal(420, ((NordpoolPrice)rez).Id);

        }

        [Fact]
        public void GetNordpoolPriceAverageTest()
        {

            NordpoolPriceController controller = new NordpoolPriceController(new MockNordpoolPriceRepository(), new NordpoolPriceService(new MockNordpoolPriceRepository()));

            var respons = (OkObjectResult)controller.GetNordpoolPriceAverage(new DateTime(1999, 12, 12, 12, 0, 0));
            var rez = respons.Value;

            Assert.Equal(5.5, (double)rez);

        }
    }
}