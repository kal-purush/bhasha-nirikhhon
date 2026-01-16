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
    public interface VoucherStatusController
    {
        List<VoucherStatus> GetAllVoucherStatus();
    }

    public class VoucherStatusControllerImpl : VoucherStatusController
    {
        DBConnection dBConnection;
        VoucherStatusDAO voucherStatusDAO = DAOFactory.createVoucherStatusDAO();
        public List<VoucherStatus> GetAllVoucherStatus()
        {
            try
            {
                dBConnection = new DBConnection();
                List<VoucherStatus> VoucherStatus = voucherStatusDAO.GetAllVoucherStatus(dBConnection);

                return VoucherStatus;
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