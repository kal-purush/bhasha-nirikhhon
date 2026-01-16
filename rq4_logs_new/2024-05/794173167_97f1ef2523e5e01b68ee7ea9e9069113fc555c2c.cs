using WebGeoInfrastructure.DTOs.Storage;
using WebGeoInfrastructure.Helpers;
using WebGeoInfrastructure.Interfaces.Repositories;
using WebGeoInfrastructure.Interfaces.Services;

namespace WebGeo.BLL.Services
{
    public class StorageService : IStorageService
    {
        private readonly IStorageRepository _storageRepository;
        public StorageService(IStorageRepository storageRepository)
        {

            _storageRepository = storageRepository;

        }
        public async Task<MessagingHelper> Create(CreateStorageDTO createStorage)
        {
            MessagingHelper response = new MessagingHelper();
            try
            {
                var create = await _storageRepository.Create(createStorage.toEntity());
                if (!create)
                {
                    response.Success = false;
                    response.Message = "Cannot create this storage";
                    return response;
                }
                response.Success = true;
            }
            catch (Exception ex)
            {
                response.Success = false;
                response.Message = ex.Message;
            }
            return response;
        }
    }
}