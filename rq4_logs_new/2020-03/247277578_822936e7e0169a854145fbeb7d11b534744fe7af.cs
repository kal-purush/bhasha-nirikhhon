using ControlSystem.BL.Device.Interfaces;
using ControlSystem.Contracts.Responses;
using ControlSystem.DAL.Device.Interfaces;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace ControlSystem.BL.Device.Services
{
    public class DeviceService : IDeviceService
    {
        private readonly IDeviceRepository _deviceRepository;

        public DeviceService(IUnitOfWork unitOfWork)
        {
            _deviceRepository = unitOfWork.DeviceRepository;
        }

        public async Task<CreateDeviceResult> CreateDevice(Contracts.Entities.Device device)
        {
            return await _deviceRepository.InsertAsync(device);
        }

        public async Task UpdateDevice(Contracts.Entities.Device device)
        {
            await _deviceRepository.UpdateAsync(device);
        }

        public async Task<Contracts.Entities.Device> GetDeviceBy(Expression<Func<Contracts.Entities.Device, bool>> expression)
        {
            return await _deviceRepository.GetDeviceByAsync(expression);
        }
    }
}