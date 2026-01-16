using System;
using JetBrains.Annotations;

namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    /// Marks Entity type has hierarchy
    /// </summary>    
    [AttributeUsage(AttributeTargets.Class| AttributeTargets.Property)]
    public class HierarchyAttribute: Attribute
    {
        /// <summary>
        /// is entity/property has hierarchy 
        /// </summary>         
        public bool HasHierarchy {get;set;}
        /// <summary>
        /// Marks Entity type has hierarchy 
        /// </summary> 
        public HierarchyAttribute()
        {
            this.HasHierarchy = true;
        }
        /// <summary>
        /// Marks Entity type has hierarchy 
        /// </summary> 
        public HierarchyAttribute(bool val)
        {
            this.HasHierarchy = val;
        }        
    }
}