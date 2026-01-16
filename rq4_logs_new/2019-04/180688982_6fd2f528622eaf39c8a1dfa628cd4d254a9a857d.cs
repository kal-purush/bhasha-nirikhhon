using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MVCswitchback.Models.Interfaces
{
    public interface ITrail
    {
        //Create a trail
        Task CreateTrail(Trail trail);

        //Edit a trail
        Task EditTrail(int id, [Bind("")] Trail trail);

        //View a single trail
        Task<Trail> GetTrail(int id);

        //Delete trail
        Task<Trail> DeleteTrail(int id);

        bool HotelExists(int id);

    }
}