using System;
using System.Linq.Expressions;
using Core.MVVM.Bindings.Interfaces;

namespace Core.MVVM.Bindings
{
    public static class BindingContextOwnerExtensions
    {
        public static BindingContextSet<TTarget, TSource> CreateBindingSet<TTarget, TSource>(
            this TTarget target)
            where TTarget : class, IBindingContextOwner 
        {
            return new BindingContextSet<TTarget, TSource>(target);
        } 
        
        public static string GetPropertyMemberName(this Expression body)
        {
            var memberExpression = body switch
            {
                UnaryExpression unaryExpression => (MemberExpression)unaryExpression.Operand,
                MemberExpression expression => expression,
                _ => null
            };

            if (memberExpression == null) throw new ArgumentNullException(nameof(memberExpression));
            return memberExpression.Member.Name;
        }
        
        public static object GetPropertyValue(this object source, string propertyName)
        {
            return source.GetType().GetProperty(propertyName)?.GetValue(source, null);
        }
        
        public static void SetPropertyValue(this object source, string propertyName, object value)
        {
            source.GetType().GetProperty(propertyName)?.SetValue(source, value, null);
        }
    }
}