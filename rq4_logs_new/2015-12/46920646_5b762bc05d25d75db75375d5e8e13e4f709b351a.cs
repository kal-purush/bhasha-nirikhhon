using System;
using System.Data;
using DAO;

namespace BUS
{
    public class KhuVucBUS : IDisposable
    {
        private readonly KhuVucDAO _khuvucDAO = new KhuVucDAO();

        public void Dispose()
        {
            _khuvucDAO.CloseConnection();
        }

        public DataTable ListKhuVuc()
        {
            return _khuvucDAO.ListKhuVuc();
        }

        public DataTable ListBan(string khuvuc)
        {
            return _khuvucDAO.ListBan(khuvuc);
        }

        public void DeleteKhuVuc(string khuvuc)
        {
            _khuvucDAO.DeleteKhuVuc(khuvuc);
        }

        public void InsertKhuVuc(string khuvuc)
        {
            _khuvucDAO.InsertKhuVuc(khuvuc);
        }

        public void InsertBan(string khuvuc, string tenban)
        {
            _khuvucDAO.InsertBan(khuvuc, tenban);
        }

        public void DeleteBan(string masoban)
        {
            _khuvucDAO.DeleteBan(masoban);
        }
    }
}