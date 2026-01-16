using System.Data;

using PicnicOrm.Ado.Factories;
using PicnicOrm.Dapper.Demo.Models;

namespace PicnicOrm.Dapper.Demo.Factories
{
    public class AddressFactory : IEntityFactory<Address>
    {
        #region Interfaces

        public Address Create(DataRow dataRow)
        {
            return new Address
                   {
                       Id = (int)dataRow["Id"],
                       City = dataRow["City"].ToString(),
                       State = dataRow["State"].ToString(),
                       Street = dataRow["Street"].ToString(),
                       PostalCode = dataRow["PostalCode"].ToString()
                   };
        }

        #endregion
    }
}