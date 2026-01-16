using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using PunkHouseReal.Data;
using PunkHouseReal.Models;
using PunkHouseReal.Models.BindingModels;
using PunkHouseReal.Services.DataAccess;
using PunkHouseReal.Services.DataAccess.Interfaces;

namespace PunkHouseReal.Controllers
{
    [Produces("application/json")]
    [Route("api/House")]
    public class HouseApiController : Controller
    {
        #region properties
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly IHouseDataAccess _houseDataAccess;
        private readonly IHouseMateDataAccess _houseMateDataAccess;
        
        #endregion

        #region constructors
        public HouseApiController(UserManager<ApplicationUser> userManager, IHouseDataAccess houseDataAccess, IHouseMateDataAccess houseMateDataAccess)
        {
            _userManager = userManager;
            _houseDataAccess = houseDataAccess;
            _houseMateDataAccess = houseMateDataAccess;

            _houseDataAccess.Initialize();
            
        }
        #endregion

        #region public methods
        /// <summary>
        /// get a list of all houses
        /// </summary>
        /// <returns>list of all houses</returns>
        [HttpGet]
        public IEnumerable<House> GetHouses()
        {
            return _houseDataAccess.GetAll();
        }

        /// <summary>
        /// Create a new house and become a member of it
        /// </summary>
        /// <param name="model"></param>
        /// <returns>House created</returns>
        [HttpPost, Route("")]
        [Authorize]
        public async Task<IActionResult> CreateHouse([FromBody]HouseBindingModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    //get current user
                   // var currentUserId = _userManager.GetUserId(HttpContext.User);
                    var houseMate = _houseMateDataAccess.GetHouseMate(model.CreatorId);

                    if (houseMate.HouseId != 0)
                    {
                        throw new Exception("houseMate already is a member of a house");
                    }

                    //first add house to db
                    House house = new House();
                    ParseHouseFields(house, model);
                    House houseAdded = _houseDataAccess.AddHouse(house);

                    //Add houseId to houseMate in db
                    await _houseMateDataAccess.UpdateHouseId(houseMate, houseAdded.Id);

                    return Ok(houseAdded);

                }
                catch (Exception)
                {

                    throw new Exception("there was an error adding the house");
                }
            }

            return BadRequest("Shitty request");
        }
        #endregion

        #region private helper methods
        private void ParseHouseFields(House house, HouseBindingModel model)
        {
            house.Name = model.Name;
            house.Address1 = model.Address1;
            house.Address2 = model.Address2;
            house.City = model.City;
            house.Zip = model.Zip;
            house.State = model.State;
            house.CreatorId = model.CreatorId;
        }
        #endregion
    }
}