namespace ShopPhone.Services.Owners
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public interface IOwnerService
    {
        public bool IsOwner(string userId);
        public int GetIdByUser(string userId);
    }
}