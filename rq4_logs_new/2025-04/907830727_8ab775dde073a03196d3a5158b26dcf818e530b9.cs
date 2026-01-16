using EasyTravel.Domain.Entites;
using EasyTravel.Domain;
using EasyTravel.Domain.Repositories;
using EasyTravel.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EasyTravel.Infrastructure.Repositories
{
    public class RecommendationRepository : IRecommendationRepository
    {
        private readonly ApplicationDbContext _context;
        public RecommendationRepository(ApplicationDbContext context)
        {
            _context = context;
        }
        
        public async Task<IEnumerable<RecommendationDto>> GetRecommendationsAsync(string type, int count = 5)
        {
            switch (type.ToLower())
            {
                case "hotels":
                    var hotels = await _context.Hotels.GetTopRatedAsync(count);
                    return hotels.Select(h => new RecommendationDto
                    {
                        Title = h.Name,
                        ImageUrl = h.Image,
                        Description = h.Description,
                        Rating = h.Rating,
                        Location = h.Address
                    });

                case "buses":
                    var buses = await _context.Buses.GetTopRatedAsync(count);
                    return buses.Select(b => new RecommendationDto
                    {
                        Title = b.OperatorName,
                        ImageUrl = b.ImagePath == null ? "https://t4.ftcdn.net/jpg/02/69/47/51/360_F_269475198_k41qahrZ1j4RK1sarncMiFHpcmE2qllQ.jpg" : b.ImagePath,
                        Description = "From: " + b.From+", To: " + b.To,
                        Rating = 5,
                        Location = "From: " + b.From + ", To: " + b.To
                    });

                case "photographers":
                    var photographers = await _context.Photographers.GetTopRatedAsync(count);
                    return photographers.Select(p => new RecommendationDto
                    {
                        Title = $"{p.FirstName} {p.LastName}",
                        ImageUrl = p.ProfilePicture,
                        Description = p.Specialization,
                        Rating = (double) p.Rating,
                        Location = p.PreferredLocations
                    });

                default:
                    return Enumerable.Empty<RecommendationDto>();
            }
        }
    }
    public static class DbSetExtensions
    {
        public static async Task<List<Hotel>> GetTopRatedAsync(this DbSet<Hotel> hotels, int count)
        {
            return await hotels
                .OrderByDescending(h => h.Rating)
                .Take(count)
                .ToListAsync();
        }

        public static async Task<List<Bus>> GetTopRatedAsync(this DbSet<Bus> buses, int count)
        {
            return await buses
                .OrderByDescending(b => b.Price)
                .Take(count)
                .ToListAsync();
        }

        public static async Task<List<Photographer>> GetTopRatedAsync(this DbSet<Photographer> photographers, int count)
        {
            return await photographers
                .OrderByDescending(p => p.Rating)
                .Take(count)
                .ToListAsync();
        }
    }

}