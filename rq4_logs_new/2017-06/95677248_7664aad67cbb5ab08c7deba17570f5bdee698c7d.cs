using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ShopSim.Data.Infrastructure;
using ShopSim.Data.Repositories;
using ShopSim.Model.Models;

namespace ShopSim.Service
{
    public interface ISimNetworkService
    {
        void Add(SimNetwork simNetwork);
        void Update(SimNetwork simNetwork);
        void Delete(int id);
        IEnumerable<SimNetwork> GetAll();
        SimNetwork GetById(int id);
        void SaveChanges();
    }
    public class SimNetworkService : ISimNetworkService
    {
        ISimNetworkRepository _simNetworkRepository;
        IUnitOfWork _unitOfWork;

        public SimNetworkService(ISimNetworkRepository simNetworkRepository, IUnitOfWork unitOfWork)
        {
            this._simNetworkRepository = simNetworkRepository;
            this._unitOfWork = unitOfWork;
        }

        public void Add(SimNetwork simNetwork)
        {
            _simNetworkRepository.Add(simNetwork);
        }

        public void Delete(int id)
        {
            _simNetworkRepository.Delete(id);
        }

        public IEnumerable<SimNetwork> GetAll()
        {
            return _simNetworkRepository.GetAll(new string[] { "FirstNumbers" });
        }

        public SimNetwork GetById(int id)
        {
            return _simNetworkRepository.GetSingleById(id);
        }

        public void SaveChanges()
        {
            _unitOfWork.Commit();
        }

        public void Update(SimNetwork simNetwork)
        {
            _simNetworkRepository.Update(simNetwork);
        }
    }
}