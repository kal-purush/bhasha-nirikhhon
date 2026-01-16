using Microsoft.AspNetCore.Routing.Constraints;
using Microsoft.Extensions.Logging;
using SpaceParkBackend.Database;
using SpaceParkBackend.Models;
using SpaceParkBackend.Repos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SpaceParkBackend.Repos
{
    public class ParkinglotRepo : Repository, IParkinglotRepo
    {
        public ParkinglotRepo(SpaceparkContext spaceparkContext, ILogger<ParkinglotRepo> logger) : base(spaceparkContext, logger)
        {}

        public async Task<ICollection<Parkinglot>> GetParkingLots()
        {
            _logger.LogInformation("Getting Parkinglots");

            //IQueryable<Parkinglot> query = 
        }
    }
}