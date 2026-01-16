using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JKEY_COMMON.Entities;
using JKEY_DL.DataContext;
using Microsoft.Extensions.Logging;

namespace JKEY_BL.BL.Page_BL

{
    public class Page_BL:IPage_BL
    {
        private IPage_BL _pageBL;
        private readonly DataContext _context;
        public Page_BL( DataContext dbContext, IPage_BL pageBL)
        {
            _pageBL = pageBL;
            _context = dbContext;
        }

        public IEnumerable<Page> GetAllPage() {
            var listP = _context.Page.OrderByDescending(x => x.UserCreated).ToList();
            return listP;
        }

    }
}