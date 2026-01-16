using Application.Repositories;
using Domain;
using DTOs;
using Microsoft.EntityFrameworkCore;

namespace Application.Services.Implementations;

public class RoomServiceImp : RoomService
{
    private readonly RoomRepository _roomRepository;

    public RoomServiceImp(RoomRepository roomRepository)
    {
        _roomRepository = roomRepository;
    }

    public IEnumerable<Room> SearchRooms(RoomSearchDTO search)
    {
        var query = _roomRepository
            .Query()
            .Include(q => q.Hotel)
            .ThenInclude(q => q.Address)
            .AsQueryable();
        
        if (search.CityName != null)
        {
            query = query.Where(q => q.Hotel.Address.City.Contains(search.CityName));
        }

        if (search.MaximumPrice != null)
        {
            query = query.Where(q => q.DailyPrice >= search.MinimumPrice);
        }

        if (search.MinimumPrice != null)
        {
            query = query.Where(q => q.DailyPrice <= search.MaximumPrice);
        }

        if (search.MinimumRate != null)
        {
            query = query.Where(q => q.Hotel.Rate >= search.MinimumRate);
        }
        
        return query.ToList();
    }
}