using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Standard.Tool.Platform.Common.AttributeEx
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
    public class PropertyAttribute : Attribute
    {
        public PropertyAttribute()
        {
            Alias = string.Empty;
        }
        public string Alias
        {
            get;
            set;
        }

        public bool IsOpenAliasCheck
        {
            get; set;
        }
    }
}