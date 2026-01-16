using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTestProject1
{
    public class Foo
    {
        //Methods
        public static readonly String _Version = "Version";
        public static readonly String _VersionStatic = "VersionStatic";
        public static readonly String _NoPublicVersion = "NoPublicVersion";
        public static readonly String result = "StaticMethodTodString";

        // Instances
        public static readonly String instanceToString = "instance";
        public static readonly String instanceStaticToString = "instanceStatic";
        public static readonly String noPublicInstanceToString = "noPublicInstance";

        public static readonly String StaticString = "VersionStatic";
        public static readonly String VersionPublic = "VersionPublic";
        public static readonly String VersionNoPublic = "VersionNoPublic";
       
        //Instances 
        public Foo instance;
        public static Foo instanceStatic;
        private Foo noPublicInstance;


        public static String VersionStatic
        {
            get;
            set;
        }

        public String Version
        {
            get;
            set;
        }

        private String NoPublicVersion
        {
            get;
            set;
        }

        public Boolean State { get; set; }

        public Foo()
        {
            VersionStatic = StaticString;
            Version = VersionPublic;
            NoPublicVersion = VersionNoPublic;
            instance = this;
            instanceStatic = this;
            noPublicInstance = this;
        }

        public Foo(Boolean state)
        {
            this.State = state;
        }

    }
}