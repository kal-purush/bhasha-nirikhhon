
using System;

namespace Microsoft.EntityFrameworkCore
{
    /// <summary>
    /// This is postgres specific attribute for use xmin system column as concurency token 
    /// </summary>       
    [AttributeUsage(AttributeTargets.Class)]
    public class UseXminAsConcurrencyTokenAttribute: Attribute
    {
    /// <summary>
    /// This is postgres specific attribute for use xmin system column as concurency token 
    /// </summary>        
        public UseXminAsConcurrencyTokenAttribute(){}        
    }    
}