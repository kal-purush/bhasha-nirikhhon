using System;
using Integratieproject1.DAL.Repositories;
using Integratieproject1.Domain.IoT;

namespace Integratieproject1.BL.Managers
{
    public class IoTManager
    {
        private IoTRepository ioTRepository;
        private UnitOfWorkManager unitOfWorkManager;

        public IoTManager()
        {
            unitOfWorkManager = new UnitOfWorkManager();
            ioTRepository = new IoTRepository(unitOfWorkManager.UnitOfWork);
        }
        public IoTManager( UnitOfWorkManager unitOfWorkManager)
        {
            if (unitOfWorkManager == null)
                throw new ArgumentNullException("unitOfWorkManager");

            this.unitOfWorkManager = unitOfWorkManager;
            ioTRepository = new IoTRepository(this.unitOfWorkManager.UnitOfWork);
        }

        public void DeleteIoTSetup(IoTSetup ioTSetup)
        {
            ioTRepository.RemoveIoTSetup(ioTSetup);
            unitOfWorkManager.Save();
        }
    }
}