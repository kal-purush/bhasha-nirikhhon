using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Studio_Inventory_API;
using Studio_Inventory_API.Models;
using Studio_Inventory_API.Repositories;

namespace Studio_Inventory_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RentalController : ControllerBase
    {
        IRepository<Rental> rentalRepo;

        public RentalController(IRepository<Rental> context)
        {
            this.rentalRepo = context;
        }

        // GET: api/Rental
        [HttpGet]
        public IEnumerable<Rental> GetRental()
        {
            return rentalRepo.GetAll();
        }

        // GET: api/Rental/5
        [HttpGet("{id}")]
        public Rental GetRental(int id)
        {
            return rentalRepo.GetById(id);
        }

        // PUT: api/Rental/5
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPut("{id}")]
        public Rental PutRental(int id, Rental rental)
        {
            if (id != rental.Id)
            {
                return null;
            }
            rentalRepo.Update(rental);
            return rental;
        }

        // POST: api/EquipmentList
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPost]
        public Rental PostRental([FromBody] Rental rental)
        {
            rentalRepo.Create(rental);
            return rental;
        }

        // DELETE: api/EquipmentList/5
        [HttpDelete("{id}")]
        public string DeleteRental(int id)
        {
            var rental = rentalRepo.GetById(id);
            rentalRepo.Delete(rental);
            return "Your reuqest has been denied!";
        }


    }
}