using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HandlerDemo
{
    public abstract class RoleAbstract
    {
        public RoleAbstract NextRole { get; set; }

        public string name { get; set; }
        public RoleAbstract(string strName)
        {
            name = strName;
        }

        public abstract void Behaviour(BehaviourRequest request);
    }
   public class ManagerRole : RoleAbstract
    {
        public ManagerRole(string strName) : base(strName)
        {
        }

        public override void Behaviour(BehaviourRequest request)
        {
            if (request.TxTNumber<5)
            {
                request.StrResult = "ManagerRole:OK";

            }
            else if (NextRole!=null)
            {
                NextRole.Behaviour(request);
            }
        }
    }

    public class PresidentRole : RoleAbstract
    {
        public PresidentRole(string strName) : base(strName)
        {
        }

        public override void Behaviour(BehaviourRequest request)
        {
            if (request.TxTNumber >5&& request.TxTNumber<15)
            {
                request.StrResult = "PresidentRole:OK";

            }
            else if (NextRole != null)
            {
                NextRole.Behaviour(request);
            }

        }
    }

    public class PresigengtRole : RoleAbstract
    {
        public PresigengtRole(string strName) : base(strName)
        {
        }

        public override void Behaviour(BehaviourRequest request)
        {
            if (request.TxTNumber > 15&& request.TxTNumber <30)
            {
                request.StrResult = "PresigengtRole:OK";

            }
            else
            {
                request.StrResult = "No person can deal it ";
            }
        }
    }

}