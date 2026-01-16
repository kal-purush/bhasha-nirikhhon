using System.Linq;

namespace AuthorizeLocker.DBLayer
{
    public class TestDbManager
    {
        private MemoryDataBase Db => MemoryDataBase.Instance;
        public bool Login(string login, string password)
        {
            return Db.UsersStorage.Any(u => u.Login == login && u.Password == password);
        }

        public void Reset()
        {
            Db.AttemptsStorage.Clear();
            Db.LocksStorage.Clear();
            Db.UnlocksStorage.Clear();
        }
    }
}