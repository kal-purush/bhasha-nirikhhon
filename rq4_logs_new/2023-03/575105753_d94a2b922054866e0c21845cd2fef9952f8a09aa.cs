using ManPowerCore.Common;
using ManPowerCore.Domain;
using ManPowerCore.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ManPowerCore.Controller
{
    public interface SalaryIncrementController
    {
        int Save(SalaryIncrement salaryIncrement);

        int Update(SalaryIncrement salaryIncrement);

        List<SalaryIncrement> GetAllSalaryIncrement();
    }

    public class SalaryIncrementControllerImpl : SalaryIncrementController
    {
        DBConnection dBConnection;
        SalaryIncrementDAO salaryIncrementDAO = DAOFactory.createSalaryIncrementDAO();
        public int Save(SalaryIncrement salaryIncrement)
        {
            try
            {
                dBConnection = new DBConnection();
                return salaryIncrementDAO.Save(salaryIncrement, dBConnection);
            }
            catch (Exception)
            {
                dBConnection.RollBack();
                throw;
            }
            finally
            {
                if (dBConnection.con.State == System.Data.ConnectionState.Open)
                    dBConnection.Commit();
            }
        }

        public int Update(SalaryIncrement salaryIncrement)
        {
            try
            {
                dBConnection = new DBConnection();
                return salaryIncrementDAO.Update(salaryIncrement, dBConnection);
            }
            catch (Exception)
            {
                dBConnection.RollBack();
                throw;
            }
            finally
            {
                if (dBConnection.con.State == System.Data.ConnectionState.Open)
                    dBConnection.Commit();
            }
        }

        public List<SalaryIncrement> GetAllSalaryIncrement()
        {
            try
            {
                dBConnection = new DBConnection();
                return salaryIncrementDAO.GetAllSalaryIncrement(dBConnection);
            }
            catch (Exception)
            {
                dBConnection.RollBack();
                throw;
            }
            finally
            {
                if (dBConnection.con.State == System.Data.ConnectionState.Open)
                    dBConnection.Commit();
            }
        }
    }
}