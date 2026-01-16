using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Security.Claims;
using Microsoft.AspNet.Identity;
using PlaceSharer.BLL.DTO;
using PlaceSharer.BLL.Infrastructure;
using PlaceSharer.BLL.Interfaces;
using PlaceSharer.DAL.Entities;
using PlaceSharer.DAL.Interfaces;

namespace PlaceSharer.BLL.Services
{
    public class PlaceService : IPlaceService
    {
        IUnitOfWork Database { get; set; }

        public PlaceService(IUnitOfWork db)
        {
            Database = db;
        }

        public async Task<OperationDetails> CreateAsync(PlaceDTO placeDto)
        {
            ApplicationUser currentUser = await Database.UserManager.FindByIdAsync(placeDto.UserId);
            if (currentUser != null)
            {
                Place place = new Place
                {
                    Name = placeDto.Name,
                    Description = placeDto.Description,
                    UserId = placeDto.UserId,
                };

                Location location = new Location
                {
                    Id = place.Id,
                    GeoLat = placeDto.GeoLat,
                    GeoLong = placeDto.GeoLong
                };

                currentUser.Places.Add(place);
                //Place

                //await Database.SaveAsync();
                return new OperationDetails(true, "Place added", "");
            }
            return new OperationDetails(false, "User with this Id already exists", "Id");
        }
    }
}