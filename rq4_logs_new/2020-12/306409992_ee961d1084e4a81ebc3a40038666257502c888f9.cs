using Backend.Repository.MySQL;
using Backend.Repository.MySQL.Stream;
using Model.AllActors;
using Model.Patient;
using Repository.IDSequencer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Backend.Repository.UsersRepository
{
    public class ManagerRepository : MySQLRepository<Manager, int>, IManagerRepository
    {
        private static ManagerRepository instance;

        public static ManagerRepository Instance()
        {
            if (instance == null)
            {
                instance = new ManagerRepository(new MySQLStream<Manager>(), new IntSequencer());
            }
            return instance;
        }

        public ManagerRepository(IMySQLStream<Manager> stream, ISequencer<int> sequencer)
            : base(stream, sequencer)
        {
        }
    }
}
