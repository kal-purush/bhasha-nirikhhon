using System;
using JetBrains.Annotations;

namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    /// Marks Entity type enable fluid table name and set the start symbols of database table name 
    /// </summary>    
    [AttributeUsage(AttributeTargets.Class)]
    public class FluidNameAttribute: Attribute
    {
        [NotNull]
        public string StartsWith {get;set;}

        public FluidNameAttribute(string startsWith)
        {
            this.StartsWith = startsWith;
        }
    }
    /// <summary>
    /// Define starts symbols of properties fields name 
    /// </summary>    
    [AttributeUsage(AttributeTargets.Class|AttributeTargets.Property)]
    public class FluidPropertyNameAttribute: Attribute
    {
        [NotNull]
        public string StartsWith {get;set;}

        public FluidPropertyNameAttribute(string startsWith)
        {
            this.StartsWith = startsWith;
        }
    }
    /// <summary>
    /// Disable fluid names for property 
    /// </summary>       
    [AttributeUsage(AttributeTargets.Property)]
    public class NoFluidNameAttribute: Attribute
    {
        public bool NoApply {get;set;}
        public NoFluidNameAttribute(bool val)
        {
            this.NoApply = val;
        }
        public NoFluidNameAttribute()
        {
            this.NoApply = true;
        }        
    }        
}