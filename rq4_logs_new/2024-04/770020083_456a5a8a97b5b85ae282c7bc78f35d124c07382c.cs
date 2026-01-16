using Domain.Entities;
using DTOs;
using Infra.Repositories;

namespace Application.Services.Implementations;

public class RoomServiceImp : RoomService
{
    private readonly RoomRepository _roomRepository;

    public RoomServiceImp(RoomRepository roomRepository)
    {
        _roomRepository = roomRepository;
    }

    public IEnumerable<Room> Search(RoomSearchDTO dto)
    {
        var query = _roomRepository.Query();
        if (dto.MinimumRate != null)
        {
            query = query.Where(q => q.Hotel.Rate >= dto.MinimumRate);
        }

        if (dto.MinimumPrice != null)
        {
            query = query.Where(q => q.Price >= dto.MinimumPrice);
        }

        if (dto.MaximumPrice != null)
        {
            query = query.Where(q => q.Price <= dto.MaximumPrice);
        }

        if (dto.CityName != null)
        {
            query = query.Where(q => q.Hotel.Address.City.Contains(dto.CityName));
        }

        return query.ToList();
    }
}