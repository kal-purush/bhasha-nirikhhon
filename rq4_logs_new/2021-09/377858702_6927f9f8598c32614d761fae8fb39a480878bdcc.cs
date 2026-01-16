using AutoMapper;
using VetClinic.Core.Interfaces.Services;
using Microsoft.Extensions.Configuration;
using Moq;
using VetClinic.BLL.Services;
using VetClinic.Core.Interfaces.Repositories;
using VetClinic.WebApi.Controllers;
using System.Collections.Generic;
using VetClinic.Core.Entities;
using Xunit;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using Microsoft.AspNetCore.Http;
using System.IO;
using System.Text;
using VetClinic.WebApi.ViewModels.PetViewModels;
using VetClinic.WebApi.Mappers;
using System.Collections.Specialized;

namespace VetClinic.WebApi.Tests.Controllers
{
    public class PetImageControllerTests
    {
        private readonly IMapper _mapper;
        private readonly Mock<IBlobService> _mockIblobService = new Mock<IBlobService>();
        private readonly Mock<IPetImageRepository> _mockPetImageRepository = new Mock<IPetImageRepository>();
        private readonly Mock<IConfiguration> _mockConfiguration = new Mock<IConfiguration>();
        private readonly IPetImageService _petImageService;

        private readonly PetsImagesController _petsImagesController;
        private readonly Mock<ControllerContext> _mockControllerContext=new Mock<ControllerContext>();

        private readonly List<PetImage> _petImages;

        public PetImageControllerTests()
        {
            if (_mapper == null)
            {
                var mappingConfig = new MapperConfiguration(mc =>
                {
                    mc.AddProfile(new PetImageMapperProfile());
                });
                _mapper = mappingConfig.CreateMapper();
            }

            _petImageService = new PetImageService(_mockPetImageRepository.Object);
           
            _petsImagesController = new PetsImagesController(_mockIblobService.Object, _petImageService, _mapper, _mockConfiguration.Object);

            //_petImages = GetPetImages();
        }

        private List<PetImage> GetPetImages()
        {
            return new List<PetImage>
            {
                new PetImage
                {
                    Id=1,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=2,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=3,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=4,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=5,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=6,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=7,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=8,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=9,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                },
                new PetImage
                {
                    Id=10,
                    Path="https://blobuploadsample21.blob.core.windows.net/testcontainer/0533b353-d73e-4819-950b-0188687ffa40",
                    PetId=1
                }
            };
        }

        [Fact]
        public async Task InsertPetImage_InsertNewImage_IfCorrectData()
        {
            //Arrange
            //IFormFile file = new FormFile(new MemoryStream(Encoding.UTF8.GetBytes("This is a test file")), 0, 0, "Data", "test.txt");

            FormFile file = new FormFile(new MemoryStream(Encoding.UTF8.GetBytes("dummy image")), 0, 0, "Data", "image.png");
            var newPetImage = new PetImage
            {
                Id = 11,
                PetId = 11
            };
            var newPetImage1 = new PetImageViewModel
            {
                PetId = 0
            };

            ////testOption
            ////NameValueCollection form = new NameValueCollection();
            ////form["data"] = JsonConvert.SerializeObject(newPetImage);
            //var formCol = new FormCollection(new Dictionary<string, Microsoft.Extensions.Primitives.StringValues>
            //    {
            //        { "Data", JsonConvert.SerializeObject(newPetImage) }
            //    });

            //_mockControllerContext.Setup(frm => frm.HttpContext.Request.Form)
            //    .Returns(formCol);


            ////_mockControllerContext.Setup(frm => frm.HttpContext.Request.Form["data"])
            ////    .Returns(JsonConvert.SerializeObject(newPetImage));

            //_mockPetImageRepository.Setup(x => x.InsertAsync(It.IsAny<PetImage>()));
            //_mockIblobService.Setup(x => x.UploadFileBlob(It.IsAny<string>(), It.IsAny<IFormFile>(), It.IsAny<string>()));


            var httpContext = new DefaultHttpContext();
            httpContext.Request.Headers["Data"] = JsonConvert.SerializeObject(newPetImage);


            _petsImagesController.ControllerContext = new ControllerContext()
            {
                HttpContext = httpContext
            };

            //Act
            var result = await _petsImagesController.InsertPetImage(file, newPetImage1);

            //Assert
            Assert.IsType<OkObjectResult>(result);

        }
    }
}