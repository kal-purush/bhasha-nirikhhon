using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HueLamp
{
    class NavigateWrapper
    {
        public NetworkHandler NetHandler { get; }
        public int id { get; }

        public NavigateWrapper(int id, MainViewModel model)
        {
            this.id = id;
            this.NetHandler = model.NetworkHandler;

        }

    }
}