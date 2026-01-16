using System;

namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    /// Mark the entity as has no base type
    /// This is equivalent .HasBaseType((Type)null) fluent api
    /// but it can be applied at top class hierarchy level
    /// </summary>       
    [AttributeUsage(AttributeTargets.Class)]
    public class NoBaseTypeAttribute : Attribute
    {
        /// <summary>
        /// Mark the entity as has no base type
        /// This is equivalent .HasBaseType((Type)null) fluent api 
        /// but it can be applied at top class hierarchy level
        /// </summary>         
        public NoBaseTypeAttribute() {}
    }
}