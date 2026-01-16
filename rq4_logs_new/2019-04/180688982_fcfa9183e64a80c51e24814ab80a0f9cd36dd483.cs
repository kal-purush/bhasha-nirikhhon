using Microsoft.EntityFrameworkCore;
using MVCswitchback.Data;
using MVCswitchback.Models.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MVCswitchback.Models.Services
{
    public class UserReviewsService : IUserReviews
    {
        private readonly SwitchbackDbContext _context;

        public UserReviewsService(SwitchbackDbContext context)
        {
            _context = context;
        }

        public async Task<List<UserReviews>> GetUserReviews(int id)
        {
            var list = _context.UserReviews.Where(x => x.TrailID == id);

            return await list.ToListAsync();
        }
    }
}