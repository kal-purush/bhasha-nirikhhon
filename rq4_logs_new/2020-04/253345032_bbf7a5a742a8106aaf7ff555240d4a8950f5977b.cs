using System;
using JetBrains.Annotations;

namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    /// Designate, that reference for this entity type does not allow null
    /// </summary>       
    [AttributeUsage(AttributeTargets.Class)]
    public class IsRequiredAsReferenceAttribute : Attribute
    {
        /// <summary>
        /// Designate, that reference for this entity type does not allow null
        /// </summary>          
        public IsRequiredAsReferenceAttribute() { }
    }

    /// <summary>
    /// Set default sql expression for reference this entity
    /// </summary>       
    [AttributeUsage(AttributeTargets.Class)]
    public class DefaultSQLValueForReferenceAttribute : Attribute
    {
        /// <summary>
        /// Set default sql expression for reference this entity
        /// </summary>        
        [NotNull]
        public string SQLExpression {get;set;}        
        /// <summary>
        /// Set default sql expression for reference this entity
        /// </summary>          
        public DefaultSQLValueForReferenceAttribute( string expressionSQL) 
        { 
            this.SQLExpression = expressionSQL;
        }
    }    
}