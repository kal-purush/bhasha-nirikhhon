using System.Collections.Generic;
using Leaderboard.Models;
using Leaderbord.Repositories;
using Microsoft.AspNetCore.Mvc;

namespace Leaderbord.Controllers
{
    public class SocialsController
    {
        private readonly ISocialList repository;

        public SocialsController(ISocialList provider)
        {
            repository = provider;
        }

        [HttpGet]
        public IEnumerable<Socials> GetSocials()
        {
            var socials = repository.GetSocials();
            return socials;
        }
    }
}