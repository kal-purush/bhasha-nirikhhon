
using System;

namespace PixelHunter1995
{
    public static class Utils
    {

        /// <summary>
        /// Throws an exception (with given message) if object is null.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <param name="msg"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public static T NotNull<T>(T obj, String msg, params String[] args)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(String.Format(msg, args));
            }
            return obj;
            //x NotNull(obj, typeof(ArgumentNullException), msg, args);
        }
        public static T NotNull<T>(T obj, Type exception, String msg, params String[] args)
        {
            if (obj == null)
            {
                throw ((Exception)Activator.CreateInstance(exception, String.Format(msg, args)));
            }
            return obj;
        }

    }
}