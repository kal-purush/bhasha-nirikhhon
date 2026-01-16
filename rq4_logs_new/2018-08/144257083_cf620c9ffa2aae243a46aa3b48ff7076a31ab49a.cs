using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using CoreApi.Models;
using CoreApi.DTO;
using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace CoreApi.Controllers
{
    [Authorize(Roles = "admin")]
    [Route("api/[controller]")]
    [ApiController]
    public class AddressController : ControllerBase
    {
        private readonly CoreApiContext _context;
        public AddressController(CoreApiContext contex)
        {
            _context = contex;
        }

        [HttpGet]
        public IQueryable GetUsersAddresses()
        {
            IQueryable result = from a in _context.Address
                                from u in _context.Users
                                where (
                                    a.Id == u.Address1 ||
                                    a.Id == u.Address2
                                )
                                from c in _context.Country
                                from ci in _context.City
                                from d in _context.District
                                where a.CountryId == c.Id &&
                                        a.CityId == ci.Id &&
                                        a.DistrictId == d.Id
                                select new
                                {
                                    UserName = u.Name,
                                    Country = c.Name,
                                    City = ci.Name,
                                    District = d.Name
                                };
            return result;
        }

        [HttpGet("user/{id}")]
        public IQueryable GetUserAddresses(int id)
        {
            // IQueryable result = from a in _context.Address
            //                 join c in _context.Country on a.CountryId equals c.Id
            //                 join ci in _context.City on a.CityId equals ci.Id
            //                 join d in _context.District on a.DistrictId equals d.Id
            //                 join us in _context.Users on a.Id equals us.Address1 or (bao loi bao loi bao loi)

            //                 select new {
            //                     Username = us.Name,
            //                     District = d.Name,
            //                     City = ci.Name,
            //                     Country = c.Name
            //                 };
            IQueryable result = from a in _context.Address
                                from u in _context.Users
                                where (
                                    a.Id == u.Address1 ||
                                    a.Id == u.Address2
                                )
                                from c in _context.Country
                                from ci in _context.City
                                from d in _context.District
                                where a.CountryId == c.Id &&
                                        a.CityId == ci.Id &&
                                        a.DistrictId == d.Id &&

                                        u.Id == id
                                select new
                                {
                                    Country = c.Name,
                                    City = ci.Name,
                                    District = d.Name
                                };
            IQueryable userAddresses;
            userAddresses = from u in _context.Users
                            where u.Id == id
                            select new
                            {
                                User = u,
                                Address = result
                            };

            return userAddresses;
        }

        [HttpPost]
        public ActionResult PostUserAddresses([FromBody] UserAddresses _userAddresses)
        {
            ActionResult Response = Unauthorized();
            Error Error = new Error();
            if (!ModelState.IsValid)
                return BadRequest(ModelState);

            try
            {
                List<int> AddressCurrentID = new List<int>(); ;
                List<Address> Addresses = _userAddresses.Addresses;
                foreach (var info in Addresses)
                {
                    _context.Address.Add(info);
                    _context.SaveChanges();

                    AddressCurrentID.Add(info.Id);
                }

                User User = _userAddresses.User;
                if (AddressCurrentID.Count() == 1)
                {
                    _userAddresses.User.Address1 = AddressCurrentID[0];

                    _context.Users.Add(User);
                    _context.SaveChanges();
                }
                else if (AddressCurrentID.Count() == 2)
                {
                    _userAddresses.User.Address1 = AddressCurrentID[0];
                    _userAddresses.User.Address2 = AddressCurrentID[1];

                    _context.Users.Add(User);
                    _context.SaveChanges();
                }

                Error.Id = 100;
                Error.Message = "Success!";
                Response = Ok(new
                {
                    Error = Error
                });
            }
            catch (Exception e)
            {
                Error.Id = 1000;
                Error.Message = "Cannot add user! The problems happen!";
                Error.Source = e.Message;
                Response = Ok(new
                {
                    Error = Error
                });
            }
            return Response;
        }
    }
}