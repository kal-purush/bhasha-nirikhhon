using AutoFixture;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.Http.Results;
using WebApi.Contracts;
using WebApi.Controllers;
using WebApi.Entities.DTO;
using WebApi.Entities.Models;
using WebApi2;
using WebApi2.Contracts;
using WebApi2.Entities.DTO;
using WebApi2.Repository;
using Xunit.Sdk;
using static System.Collections.Specialized.BitVector32;
using NotFoundResult = Microsoft.AspNetCore.Mvc.NotFoundResult;

namespace WebApi2Test.ControllerTest
{
    public class accountControllerTest
    {
        private readonly accountController controller;
        private readonly IUserService userContract;
        private readonly IUserRep userRep;
       
        /// <summary>
        /// controller
        /// </summary>
        public accountControllerTest()
        {
           
            userRep = new UserRep(ApiContext.inmemory());
            userContract = new UserService(userRep);
            controller = new accountController(userContract);

        }  
        /// <summary>
        /// test that method return the count of the address book
        /// </summary>
        [Fact]
        public void countTest()
        {
            accountController accountController1 = controller;
            
            
            IActionResult actionResult= controller.count();
            OkObjectResult result=actionResult as OkObjectResult;
            int model = (int)result.Value;
            Assert.NotNull(model);
            Assert.StrictEqual<int>(1,model);
            Assert.IsType<OkObjectResult>(actionResult);

        }
        /// <summary>
        /// test that method adds the new user
        /// </summary>
        [Fact]
        public void CreateUserTest_AddsUser_ReturnUserId()
        {
            accountController accountController1 = controller;
            UserDTO user = new UserDTO();
            user.UserName = "Ram1";
            user.Password = "Rio@123";
            user.FirstName = "Kavinfgcvhnlkhjgf";
            user.LastName = "Kavin@123";
            user.Address = new List<AddressDTO>();
            user.Email = new List<EmailDTO>();
            user.phones = new List<PhoneNumberDTO>();
            AddressDTO address1 = new AddressDTO()
            {

                Line1 = "182",
                Line2 = "182",
                City = "Palani",
                ZipCode = 624617,
                stateName = "Tamil Nadu",
                Type = new TypesDTO() { Key = "WORK" },
                country = new TypesDTO() { Key = "WORK" }
            };
            user.Address.Add(address1);
            EmailDTO email1 = new EmailDTO()
            {
                EmailAddress = "ram1@propel.com",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.Email.Add(email1);
            PhoneNumberDTO phoneNumber1 = new PhoneNumberDTO()
            {
                phoneNo = "9876543210",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.phones.Add(phoneNumber1);
            var actionResult = controller.CreateUser(user);
            var result = actionResult as OkObjectResult;
            string model = (string)result.Value;
            Assert.NotNull(model);  
            Assert.IsType<string>(model);
            Assert.IsType<OkObjectResult>(result);
        }
        /// <summary>
        /// test and conflict if username already exist
        /// </summary>
        [Fact]
        public void CreateUserTest_UserAlreadyExsist_ReturnConflict()
        {
            accountController accountController1 = controller;
            UserDTO user = new UserDTO();
            user.UserName = "Kavin";
            user.Password = "Rio@123";
            user.FirstName = "Kavinfgcvhnlkhjgf";
            user.LastName = "Kavin@123";
            user.Address = new List<AddressDTO>();
            user.Email = new List<EmailDTO>();
            user.phones = new List<PhoneNumberDTO>();
            AddressDTO address1 = new AddressDTO()
            {

                Line1 = "182",
                Line2 = "182",
                City = "Palani",
                ZipCode = 624617,
                stateName = "Tamil Nadu",
                Type = new TypesDTO() { Key = "WORK" },
                country = new TypesDTO() { Key = "WORK" }
            };
            user.Address.Add(address1);
            EmailDTO email1 = new EmailDTO()
            {
                EmailAddress = "ram@propel.com",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.Email.Add(email1);
            PhoneNumberDTO phoneNumber1 = new PhoneNumberDTO()
            {
                phoneNo = "9876543210",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.phones.Add(phoneNumber1);
            
            var result = controller.CreateUser(user);
            var action = result as ConflictObjectResult;
            string model=action.Value as string;
            Assert.NotNull(model);
            Assert.StrictEqual<string>("UserName already exist", model);
            Assert.IsType<ConflictObjectResult>(result);
        }
        /// <summary>
        /// test that method return conflict if email already exist
        /// </summary>
        [Fact]
        public void CreateUserTest_EmailAlreadyExsist_ReturnConflict()
        {
            accountController accountController1 = controller;
            UserDTO user = new UserDTO();
            user.UserName = "gg";
            user.Password = "Rio@123";
            user.FirstName = "Kavinfgcvhnlkhjgf";
            user.LastName = "Kavin@123";
            user.Address = new List<AddressDTO>();
            user.Email = new List<EmailDTO>();
            user.phones = new List<PhoneNumberDTO>();
            AddressDTO address1 = new AddressDTO()
            {

                Line1 = "182",
                Line2 = "182",
                City = "Palani",
                ZipCode = 624617,
                stateName = "Tamil Nadu",
                Type = new TypesDTO() { Key = "WORK" },
                country = new TypesDTO() { Key = "WORK" }
            };
            user.Address.Add(address1);
            EmailDTO email1 = new EmailDTO()
            {
                EmailAddress = "abcd@propel.com",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.Email.Add(email1);
            PhoneNumberDTO phoneNumber1 = new PhoneNumberDTO()
            {
                phoneNo = "9876543210",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.phones.Add(phoneNumber1);
            var result = controller.CreateUser(user);
            var action = result as ConflictObjectResult;
            string model = action.Value as string;
            Assert.NotNull(model);
            Assert.StrictEqual<string>("Email already exist", model);
            Assert.IsType<ConflictObjectResult>(result);
        }
        /// <summary>
        /// test that method return conflict when type is not valid
        /// </summary>
        [Fact]
        public void CreateUserTest_TypeNotFound_ReturnConflict()
        {
            accountController accountController1 = controller;
            UserDTO user = new UserDTO();
            user.UserName = "Vijay";
            user.Password = "Rio@123";
            user.FirstName = "Kavinfgcvhnlkhjgf";
            user.LastName = "Kavin@123";
            user.Address = new List<AddressDTO>();
            user.Email = new List<EmailDTO>();
            user.phones = new List<PhoneNumberDTO>();
            AddressDTO address1 = new AddressDTO()
            {

                Line1 = "182",
                Line2 = "182",
                City = "Palani",
                ZipCode = 624617,
                stateName = "Tamil Nadu",
                Type = new TypesDTO() { Key = "WORK" },
                country = new TypesDTO() { Key = "WORK" }
            };
            user.Address.Add(address1);
            EmailDTO email1 = new EmailDTO()
            {
                EmailAddress = "Vijay@propel.com",
                Type = new TypesDTO() { Key = "Nothing" },
            };
            user.Email.Add(email1);
            PhoneNumberDTO phoneNumber1 = new PhoneNumberDTO()
            {
                phoneNo = "9876543210",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.phones.Add(phoneNumber1);

            var result = controller.CreateUser(user);
            var action=result as ConflictObjectResult;
            string model = action.Value as string;
            Assert.NotNull(model);
            Assert.StrictEqual("type is not valid", model);
            Assert.IsType<ConflictObjectResult>(result);
        }
        [Fact]
        public void GetbyIdTest_displayUser_ReturnUser()
        {
            Guid guid = Guid.Parse(("2CD90236-F155-401D-9A5F-01A782A3B052"));
            var result = controller.GetById(guid);
            GetDTO user = new GetDTO();
            user.Id = guid;
            user.FirstName = "Kavin";
            user.LastName = "Raja";
            user.Email = new List<EmailGetDTO>();

            EmailGetDTO email1 = new EmailGetDTO()
            {
               
                EmailId = Guid.Parse("2CD90277-F155-401D-9A5F-01A782A3B052"),
                EmailAddress = "abcd@propel.com",
                Type =new TypesDTO() { Key="WORK"}
            };
            user.Email.Add(email1);
            user.Address = new List<AddressGetDTO>();
            AddressGetDTO address1 = new AddressGetDTO()
            {
              
                AddressId = Guid.Parse(("3CD90236-F155-401D-9A5F-01A782A3B052")),
                Line1 = "182",
                Line2 = "182",
                stateName = "Tamil Nadu",
                ZipCode = 624617,
                City = "Palani",
                country = new TypesDTO(){ Key="INDIA"},
                Type = new TypesDTO() { Key="WORK"}
            };
            user.Address.Add(address1);
            user.phones = new List<PhoneNumberGetDTO>();
            PhoneNumberGetDTO phoneNumber1 = new PhoneNumberGetDTO()
            {
               PhoneNumberId= Guid.Parse("2CD90227-F355-401D-9A5F-11A782A3B052"),
                phoneNo = "9003404873",
                Type = new TypesDTO() { Key = "WORK" }
            };
            user.phones.Add(phoneNumber1);
            OkObjectResult action = result as OkObjectResult;
            GetDTO model = action.Value as GetDTO;
            Assert.NotNull(model);
            Assert.Equal(user.Id,model.Id);
            Assert.IsType<OkObjectResult>(result);
        }
        /// <summary>
        /// test that returns Not Found if user not found
        /// </summary>
        [Fact]
        public void GetbyIdTest_userNotFound_ReturnNotFound()
        {
            Guid guid = Guid.Parse(("2CD90237-F155-401D-9A5F-01A782A3B052"));
            var result = controller.GetById(guid);
            Assert.IsType<NotFoundResult>(result);
        }
        /// <summary>
        /// test that return updated user when it updates successfully
        /// </summary>
        [Fact]
        public void UpdateTest_updateById_ReturnUser()
        {
            Guid guid = Guid.Parse(("2CD90236-F155-401D-9A5F-01A782A3B052"));
            UserDTO update = new UserDTO();
            update.UserName = "Vijay";
            update.Password = "Vijay@123";
            update.FirstName = "Kavinf";
            update.LastName = "Kavin@123";
            update.Address = new List<AddressDTO>();
            update.Email = new List<EmailDTO>();
            update.phones = new List<PhoneNumberDTO>();
            AddressDTO address1 = new AddressDTO()
            {
                Line1 = "182",
                Line2 = "182",
                City = "Palani",
                ZipCode = 624617,
                stateName = "Tamil Nadu",
                Type = new TypesDTO() { Key = "work" },
                country = new TypesDTO() { Key = "work" }
            };
            update.Address.Add(address1);
            EmailDTO email1 = new EmailDTO()
            {
                EmailAddress = "abc@propelinc.com",
                Type = new TypesDTO() { Key = "work" },
            };
            update.Email.Add(email1);

            PhoneNumberDTO phoneNumber1 = new PhoneNumberDTO()
            {
                phoneNo = "9876543210",
                Type = new TypesDTO() { Key = "work" },
            };
            update.phones.Add(phoneNumber1);

            UpdateDTO user = new UpdateDTO();
            user.Id = guid;
            user.UserName = "Vijay";
            user.Password = "Vijay@123";
            user.FirstName = "Kavinf";
            user.LastName = "Kavin@123";
            user.Address = new List<AddressDTO>();
            user.Email = new List<EmailDTO>();
            user.phoneNumber = new List<PhoneNumberDTO>();
            AddressDTO address2 = new AddressDTO()
            {
                Line1 = "182",
                Line2 = "182",
                City = "Palani",
                ZipCode = 624617,
                stateName = "Tamil Nadu",
                Type = new TypesDTO() { Key = "work" },
                country = new TypesDTO() { Key = "work" }
            };
            user.Address.Add(address2);
            EmailDTO email2 = new EmailDTO()
            {
                EmailAddress = "abc@propelinc.com",
                Type = new TypesDTO() { Key = "work" },
            };
            user.Email.Add(email2);

            PhoneNumberDTO phoneNumber2 = new PhoneNumberDTO()
            {
                phoneNo = "9876543210",
                Type = new TypesDTO() { Key = "work" },
            };
            user.phoneNumber.Add(phoneNumber2);

            var result=controller.Update(guid, update);
            var action = result as OkObjectResult;
            UpdateDTO model=action.Value as UpdateDTO; 
            Assert.NotNull(model);
            Assert.Same(user.FirstName, model.FirstName);
            Assert.IsType<OkObjectResult>(result);
        }
        /// <summary>
        /// test that method return NotFound if user doesn't exist
        /// </summary>
        [Fact]
        public void UpdateTest_userNotFound_ReturnNotFound()
        {
            Guid guid = Guid.Parse(("2CD90236-F155-401D-9A5F-01A782A3B053"));
            UserDTO user = new UserDTO();
            user.UserName = "Kavin";
            user.Password = "abc@123";
            user.FirstName = "Kavinfgcvhnlkhjgf";
            user.LastName = "Kavin@123";
            user.Address = new List<AddressDTO>();
            user.Email = new List<EmailDTO>();
            user.phones = new List<PhoneNumberDTO>();
            AddressDTO address1 = new AddressDTO()
            {
                Line1 = "182",
                Line2 = "182",
                City = "Palani",
                ZipCode = 624617,
                stateName = "Tamil Nadu",
                Type = new TypesDTO() { Key = "WORK" },
                country = new TypesDTO() { Key = "WORK" }
            };
            user.Address.Add(address1);
            EmailDTO email1 = new EmailDTO()
            {
                EmailAddress = "xyz1@propelinc.com",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.Email.Add(email1);

            PhoneNumberDTO phoneNumber1 = new PhoneNumberDTO()
            {
                phoneNo = "9876543210",
                Type = new TypesDTO() { Key = "work" },
            };
            user.phones.Add(phoneNumber1);
            var result = controller.Update(guid, user);
            Assert.IsType<NotFoundResult>(result);
        }
        /// <summary>
        /// test that method returns type not valid
        /// </summary>
        [Fact]
        public void UpdateTest_TypeNotValid_ReturnConflict()
        {
            Guid guid = Guid.Parse(("2CD90236-F155-401D-9A5F-01A782A3B052"));
            UserDTO user = new UserDTO();
            user.UserName = "Kavin";
            user.Password = "abc@123";
            user.FirstName = "Kavinfgcvhnlkhjgf";
            user.LastName = "Kavin@123";
            user.Address = new List<AddressDTO>();
            user.Email = new List<EmailDTO>();
            user.phones = new List<PhoneNumberDTO>();
            AddressDTO address1 = new AddressDTO()
            {
                Line1 = "182",
                Line2 = "182",
                City = "Palani",
                ZipCode = 624617,
                stateName = "Tamil Nadu",
                Type = new TypesDTO() { Key = "WORK" },
                country = new TypesDTO() { Key = "WORK" }
            };
            user.Address.Add(address1);
            EmailDTO email1 = new EmailDTO()
            {
                EmailAddress = "xyz1@propelinc.com",
                Type = new TypesDTO() { Key = "WORK" },
            };
            user.Email.Add(email1);

            PhoneNumberDTO phoneNumber1 = new PhoneNumberDTO()
            {
                phoneNo = "9876543210",
                Type = new TypesDTO() { Key = "working" },
            };
            user.phones.Add(phoneNumber1);
            var result = controller.Update(guid, user);
            Assert.IsType<ConflictObjectResult>(result);
        }
        /// <summary>
        /// test that method display user
        /// </summary>
        [Fact]
        public void GetAllUserTest_displayAllUser_ReturnUser()
        {
            var pagination = new Pagination()
            {
                pageNumber = 1,
                pageSize = 10,
                SortBy = "FirstName",
                SortOrder = "DSC",
            };
            GetDTO user = new GetDTO();
            user.FirstName = "Kavin";
            user.LastName = "Raja";
            user.Email = new List<EmailGetDTO>();

            EmailGetDTO email1 = new EmailGetDTO()
            {

                EmailId = Guid.Parse("2CD90277-F155-401D-9A5F-01A782A3B052"),
                EmailAddress = "abcd@propel.com",
                Type = new TypesDTO() { Key = "WORK" }
            };
            user.Email.Add(email1);
            user.Address = new List<AddressGetDTO>();
            AddressGetDTO address1 = new AddressGetDTO()
            {

                AddressId = Guid.Parse(("3CD90236-F155-401D-9A5F-01A782A3B052")),
                Line1 = "182",
                Line2 = "182",
                stateName = "Tamil Nadu",
                ZipCode = 624617,
                City = "Palani",
                country = new TypesDTO() { Key = "INDIA" },
                Type = new TypesDTO() { Key = "WORK" }
            };
            user.Address.Add(address1);
            user.phones = new List<PhoneNumberGetDTO>();
            PhoneNumberGetDTO phoneNumber1 = new PhoneNumberGetDTO()
            {
                PhoneNumberId = Guid.Parse("2CD90227-F355-401D-9A5F-11A782A3B052"),
                phoneNo = "9003404873",
                Type = new TypesDTO() { Key = "WORK" }
            };
            user.phones.Add(phoneNumber1);
            IActionResult result = controller.GetAllUser(pagination);
            OkObjectResult action = result as OkObjectResult;
            List<GetDTO> model = action.Value as List<GetDTO>;
            Assert.NotNull(model);
           Assert.Equal(1, model.Count);
            Assert.IsType<OkObjectResult>(result);
        }
        /// <summary>
        /// test that method deletes the user
        /// </summary>
        [Fact]
        public void DeleteTest_DeleteUser_ReturnSuccesslyDeleted()
        {
            Guid guid = Guid.Parse(("2CD90236-F155-401D-9A5F-01A782A3B052"));
            IActionResult result = controller.Delete(guid);
            
            Assert.IsType<OkObjectResult>(result);
        }
        /// <summary>
        /// test that returns id not found
        /// </summary>
        [Fact]
        public void DeleteTest_IdNotFound_ReturnNotFound()
        {
            IActionResult result = controller.Delete(Guid.Parse(("2CD90233-F155-401D-9A5F-01A782A3B052")));
            Assert.IsType<NotFoundResult>(result);
        }
    }
}