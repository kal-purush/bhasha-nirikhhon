using Core.DataAccess.MongoDB;
using Core.Entities.Concrete;
using DataAccess.Abstract;
using DataAccess.Concrete.MongoDB.Collections;
using Entities.Concrete;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAccess.Concrete.MongoDB
{
    public class MongoDB_UserDal : MongoDB_RepositoryBase<User, MongoDB_Context<User, MongoDB_ReceiptCollection>>, IUserDal
    {
        public List<OperationClaim> GetClaims(User user)
        {
            throw new NotImplementedException();
        }
    }
}