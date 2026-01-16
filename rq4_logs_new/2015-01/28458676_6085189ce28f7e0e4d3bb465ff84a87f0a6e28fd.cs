using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Microsoft.Ajax.Utilities;

namespace harristeq.Models
{
    public class DemoBaseClass
    {
        public class DemoPublicClass
        {
        }

        protected class DemoProtectedClass
        {
        }

        private class DemoPrivateClass
        {
        }

        public DemoPublicClass DemoPublicClassOut { get; set; }
        protected DemoPublicClass DemoPublicClassProtected { get; set; }
        private DemoPublicClass DemoPublicClassPrivate { get; set; }

        protected DemoProtectedClass DemoProtectedClassOut { get; set; }
        private DemoProtectedClass DemoProtectedClassPrivate { get; set; }
        ////won't work
        //public DemoProtectedClass DemoProtectedClassOutPublic { get; set; }

        private DemoPrivateClass DemoPrivateClassOut { get; set; }
        ////won't work
        //public DemoPrivateClass DemoPrivateClassOutPublic { get; set; }
        //protected DemoPrivateClass DemoPrivateClassOutProtected { get; set; }
    }

    public class DemoChildClass : DemoBaseClass
    {
    }

    sealed class DemoSealedBaseClass
    {
        public void DoThis()
        {
            var d = new DemoAbstractChildClass();
            d.MustHaveThis = "this";
        }
    }

    ///// <summary>
    ///// can't work
    ///// </summary>
    //public class DemoSealedChildClass : DemoSealedBaseClass
    //{
    //}

    public abstract class DemoAbstractClass
    {
        public virtual string CanHaveThis { get; set; }
        public abstract string MustHaveThis { get; set; }
    }

    public class DemoAbstractChildClass : DemoAbstractClass
    {
        public override string MustHaveThis { get; set; }
    }

}