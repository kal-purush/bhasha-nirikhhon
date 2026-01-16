using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SignalR.Data.Infrastructure
{
    public class DatabaseFactory : Disposable,IDatabaseFactory
    {

        private WunderlistContext context;

        protected override void DisposeCore()
        {
            if (context != null)
                context.Dispose();
        }

        public WunderlistContext Context
        {
            get
            {
                return context ?? (context = new WunderlistContext());
            }
        }
    }
}