using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Funccy
{
    public static class ObjectExtensions
    {
        /// <summary>
        /// Call a function inline.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TNext"></typeparam>
        /// <param name="obj"></param>
        /// <param name="f"></param>
        /// <returns></returns>
        public static TNext Chain<T, TNext>(this T obj, Func<T, TNext> f)
        {
            return f(obj);
        }

        /// <summary>
        /// Indicates if the string is not null, empty, or whitespace.
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static bool NotNullOrWhiteSpace(this string str)
        {
            return !string.IsNullOrWhiteSpace(str);
        }

        /// <summary>
        /// Inverts the result of a predicate
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="f"></param>
        /// <returns></returns>
        public static Func<T, bool> Not<T>(this Func<T, bool> f)
        {
            return x => !f(x);
        }

        /// <summary>
        /// Indicates if reference is null.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static bool IsNotNull<T>(this T obj) where T : class
        {
            return obj != null;
        }

        /// <summary>
        /// Turns a sync function into an async returning one.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="f"></param>
        /// <returns></returns>
        public static Func<T, Task<U>> Defer<T, U>(this Func<T, U> f)
        {
            return x => f(x).TaskFromResult();
        }

        /// <summary>
        /// Creates a completed task from the object.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="t"></param>
        /// <returns></returns>
        public static Task<T> TaskFromResult<T>(this T t) { return Task.FromResult(t); }

        /// <summary>
        /// Casts the result of a task to a specific type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TCast"></typeparam>
        /// <param name="t"></param>
        /// <returns></returns>
        public static async Task<TCast> CastTask<T, TCast>(this Task<T> t)
        {
            return (await t).CastObject<TCast>();
        }

        /// <summary>
        /// Casts an object to a specific type.
        /// </summary>
        /// <typeparam name="TCast"></typeparam>
        /// <param name="t"></param>
        /// <returns></returns>
        public static TCast CastObject<TCast>(this object t)
        {
            return (TCast)t;
        }

        /// <summary>
        /// Casts the return of a func to a specific type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <typeparam name="TCast"></typeparam>
        /// <param name="t"></param>
        /// <returns></returns>
        public static Func<T, TCast> CastReturn<T, U, TCast>(this Func<T, U> t)
        {
            return x => t(x).CastObject<TCast>();
        }

        /// <summary>
        /// If the object is in a collection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <param name="arr"></param>
        /// <returns></returns>
        public static bool IsIn<T>(this T obj, IEnumerable<T> arr)
        {
            return arr.Contains(obj);
        }
    }
}