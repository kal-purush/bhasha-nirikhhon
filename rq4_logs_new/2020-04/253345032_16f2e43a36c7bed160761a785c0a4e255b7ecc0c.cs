using System;
using JetBrains.Annotations;

namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    /// Set default sql expression for property
    /// </summary>       
    [AttributeUsage(AttributeTargets.Property)]
    public class DefaultSQLValueAttribute : Attribute
    {
        /// <summary>
        /// Set default sql expression for property
        /// </summary>        
        [NotNull]
        public string SQLExpression {get;set;}        
        /// <summary>
        /// Set default sql expression for property
        /// </summary>          
        public DefaultSQLValueAttribute( string expressionSQL) 
        { 
            this.SQLExpression = expressionSQL;
        }
    }    

}