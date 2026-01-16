using EVA_backend.Entities;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Web;

namespace EVA_backend.DataLayer
{
    public static class Eva18Singleton
    {
        private static DbContext _db;

        public static DbContext Db
        {
            get { if(_db == null)
                {
                    _db = new EVA18Entities();
                    return _db;
                }
                else
                {
                    return _db;
                }
            }
        }
    }
}