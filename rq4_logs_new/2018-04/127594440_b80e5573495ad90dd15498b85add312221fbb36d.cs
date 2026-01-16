/**************************************************************
 *  Filename:    CachingCallAttribute.cs
 *  Description: LWJ.Injection ClassFile
 *  @author:     WenJie Liu
 *  @version     2017/2/17
 **************************************************************/
using System; 

namespace LWJ.Injection.Aop.Caching
{

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
    public sealed class CachingCallAttribute : CallHandlerAttribute
    {

        public CachingCallAttribute()
        {
            MaxCaching = 10;
        }

        public int MaxCaching { get; set; }
        // public bool ParameterChanged
 

    }



}