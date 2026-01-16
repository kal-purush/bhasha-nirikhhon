using install.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace install.Basic.Model.Chain
{
    abstract class BaseConfigReader : ConfigReader
    {
        private ConfigReader next;

        public Config giveNext(Config modelConfig, Config newConfig)
        {
            if(next != null)
            {
                return next.read(modelConfig, newConfig);
            }
            else
            {
                throw new FinishChain();
            }
        }

        abstract public Config read(Config modelConfig, Config newConfig);

        public void setNext(ConfigReader next)
        {
            this.next = next;
        }
    }
}