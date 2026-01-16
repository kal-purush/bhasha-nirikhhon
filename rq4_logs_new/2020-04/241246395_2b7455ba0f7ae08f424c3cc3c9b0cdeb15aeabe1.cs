using System;
using System.Threading.Tasks;
using PulsePI.DataAccess.DaoInterfaces;
using PulsePI.DataContracts;
using PulsePI.Exceptions;
using PulsePI.Models;
using PulsePI.Service.ServiceInterfaces;

namespace PulsePI.Service
{
    public class HeartRateRecordService : IHeartRateRecordService
    {
        IHeartRateRecordDao _heartRateRecordDao;

        public HeartRateRecordService(IHeartRateRecordDao hrrd)
        {
            _heartRateRecordDao = hrrd;
        }

        public async Task RecordHeartRate(HeartRateRecordData hr)
        {
            try
            {
                await _heartRateRecordDao.RecordHeartRate(hr);
            }
            catch(Exception e)
            {
                throw new CustomException("Error at record heart rate service" + e);
            }
        }
    }
}