using project2._4.BL.Repositories;
using project2._4.Entities.ViewModels;
using project2._4.Entities.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace project2._4.API.Controllers
{
    public class ProfileInfoController : ApiController
    {
        [HttpGet]
        public IHttpActionResult GetProfileInfo(Guid Id)
        {
            UserRepository db = new UserRepository();
            UserInfoRepository db2 = new UserInfoRepository();
            User user = db.GetUser(Id);
            ProfileInfo profileinfo = db2.GetProfileInfo(Id);
            ProfileViewModel viewmodel = new ProfileViewModel()
            {
                Id = user.Id,
                FirstName = user.FirstName,
                LastName = user.LastName,
                Email = user.Email,
                Gender = user.Gender,
                BirthDate = user.BirthDate,
                Work = profileinfo.Werk,
                School = profileinfo.School,
                Hometown = profileinfo.Woonplaats,
                Relation = profileinfo.RelatieStatus,
                Hobby = profileinfo.Hobbies
            };

            return Ok(viewmodel);

        }

        [HttpPut]
        public IHttpActionResult UpdateProfile([FromBody] ProfileViewModel viewModel)
        {
            UserRepository db = new UserRepository();
            UserInfoRepository db2 = new UserInfoRepository();
            User user = db.GetUser(viewModel.Id);
            ProfileInfo profileinfo = db2.GetProfileInfo(viewModel.Id);
            user.FirstName = viewModel.FirstName;
            user.LastName = viewModel.LastName;
            user.Email = viewModel.Email;
            user.BirthDate = viewModel.BirthDate;
            user.Gender = viewModel.Gender;
            profileinfo.Werk = viewModel.Work;
            profileinfo.School = viewModel.School;
            profileinfo.Woonplaats = viewModel.Hometown;
            profileinfo.RelatieStatus = viewModel.Relation;
            profileinfo.Hobbies = viewModel.Hobby;

            db.UpdateUser(user);
            db2.UpdateProfileInfo(profileinfo);
            return Ok();
        }
    }
}