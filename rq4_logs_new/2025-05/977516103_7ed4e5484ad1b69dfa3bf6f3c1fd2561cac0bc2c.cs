using GoogleMaps_FinalProjectAspApi.DAL.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GoogleMaps_FinalProjectAspApi.DAL.Abstract
{
    public interface IPlaceDetailsRepository
    {
        Task AddAsync(PlaceDetails placeDetails);
        Task DefaultDeleteAsync(Guid id);
        Task<PlaceDetails> GetByIdAsync(Guid id);
        Task<IEnumerable<PlaceDetails>> GetAllAsync();
        Task UpdateAsync(PlaceDetails placeDetails);
    }
}