using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using home_swap_api.Data;
using home_swap_api.Dto;
using home_swap_api.interfaces;
using home_swap_api.Models;
using home_swap_api.Repository;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.JsonPatch;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace home_swap_api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HouseController : ControllerBase
    {
  
        private readonly IUnitOfWork uow;
        private readonly IMapper mapper;

        public HouseController(IUnitOfWork uow,IMapper mapper)
        {
            this.uow = uow;
            this.mapper = mapper;
        }


        [HttpGet]
        public async Task<IActionResult> GetHouses()
        {
            var houses = await uow.HouseRepository.GetHousesAsync();
            var housesDTO = mapper.Map<IEnumerable<HouseDTO>>(houses);

            //throw new Exception("Some unknow error");

            return Ok(housesDTO);
        }

        [HttpPost]
        public async Task<IActionResult> AddHouse([FromBody]HouseDTO houseDTO)
        {
            //throw new UnauthorizedAccessException();
            var house = mapper.Map<House>(houseDTO);


            uow.HouseRepository.AddHouse(house);
            await uow.SaveAsync();

            return StatusCode(201);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> DeleteHouse(int id)
        {
            uow.HouseRepository.DeleteHouse(id);
            await uow.SaveAsync();
            
            return Ok(id);
        }

        [HttpPut("update/{id}")]
        public async Task<IActionResult> UpdateHouse(int id , HouseDTO houseDTO)
        {
            var houseFromDb = await uow.HouseRepository.FindHouse(id);
            mapper.Map(houseDTO, houseFromDb);
            await uow.SaveAsync();

            return StatusCode(200);
        }

        [HttpPut("updateHouseGarage/{id}")]
        public async Task<IActionResult> UpdateHouse(int id, HouseUpdateDTO houseDTO)
        {
            var houseFromDb = await uow.HouseRepository.FindHouse(id);
            mapper.Map(houseDTO, houseFromDb);
            await uow.SaveAsync();

            return StatusCode(200);
        }

        [HttpPatch("update/{id}")]
        public async Task<IActionResult> UpdateHousePatch(int id, JsonPatchDocument <House> houseToPatch)
        {
            var houseFromDb = await uow.HouseRepository.FindHouse(id);
            houseToPatch.ApplyTo(houseFromDb, ModelState);
            await uow.SaveAsync();

            return StatusCode(200);
        }



    }
}