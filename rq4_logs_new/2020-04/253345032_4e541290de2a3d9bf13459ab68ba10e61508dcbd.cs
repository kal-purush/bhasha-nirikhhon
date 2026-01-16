using System;
using JetBrains.Annotations;

namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    /// Designates property as a part of index
    /// </summary>    
    [AttributeUsage(AttributeTargets.Property)]
    public class IndexAttribute: Attribute
    {
        /// <summary>
        /// Sets start substring for index name
        /// </summary>        
        [NotNull]
        public string StartsWith {get;set;}
        /// <summary>
        /// Sets the index is unique, default is false 
        /// </summary>        
        public bool IsUnique {get;set;}        
        /// <summary>
        /// Sets the property is included to index 
        /// </summary> 
        public IndexAttribute(string startsWith)
        {
            this.StartsWith = startsWith;
            this.IsUnique = false;
        }
        /// <summary>
        /// Sets the property is included to index with unique 
        /// </summary>         
        public IndexAttribute(string startsWith, bool isUnique)
        {
            this.StartsWith = startsWith;
            this.IsUnique = isUnique;
        }        
    }
}