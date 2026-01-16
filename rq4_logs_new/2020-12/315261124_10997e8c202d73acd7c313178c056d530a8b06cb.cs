using System;
using System.Linq.Expressions;
using VNNSIS.Core.Entities.PgEntities;
using VNNSIS.Core.Entities.SqlEntities;

namespace VNNSIS.Core.Specification
{
     public class SelectMenuByStringId : BaseSpecification<MenuMasterTraining>
     {
          public SelectMenuByStringId() : base()
          {
               AddOrderBy(x => x.MenuIdentity);
          }

          public SelectMenuByStringId(string role) : base(x => x.UserRole == role)
          {
               AddOrderBy(x => x.MenuIdentity);
          }
     }
}