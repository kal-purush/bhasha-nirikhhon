using System;
using System.Threading.Tasks;
using PulsePI.DataAccess.DaoInterfaces;
using PulsePI.DataContracts;
using PulsePI.Exceptions;
using PulsePI.Models;
using PulsePI.Service.ServiceInterfaces;

namespace PulsePI.Service
{
    public class BiometricService : IBiometricService
    {
        IBiometricDataDao _bio;

        public BiometricService(IBiometricDataDao bio)
        {
         _bio = bio;
        }
        public async Task CreateBiometricData(CreateAccountData cbd)
        {
            
        }
    }
}