using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HaloBiz.MyServices
{
    public interface IStrategicBusinessUnitService
    {
        Task<ApiResponse> AddStrategicBusinessUnit(StrategicBusinessUnitReceivingDTO strategicBusinessUnitReceivingDTO);
        Task<ApiResponse> GetStrategicBusinessUnitById(long id);
        Task<ApiResponse> GetStrategicBusinessUnitByName(string name);
        Task<ApiResponse> GetAllStrategicBusinessUnit();
        Task<ApiResponse> UpdateStrategicBusinessUnit(long id, StrategicBusinessUnitReceivingDTO strategicBusinessUnitReceivingDTO);
        Task<ApiResponse> DeleteStrategicBusinessUnit(long id);
    }
}