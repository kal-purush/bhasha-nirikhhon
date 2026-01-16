using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PolyRents.helpers
{
    public class EnumUtil
    {
        private static Logger myLogger;

        static Logger logger
        {
            get
            {
                if (myLogger == null)
                {
                    myLogger = Logger.getInstance();
                }
                return myLogger;

            }
        }

        public static T ParseEnum<T>(string value)
        {
            try
            {
                return (T)Enum.Parse(typeof(T), value, true);
            }
            catch
            {
                logger.Error("Tried parsing value(" + value + ") for enum of type(" + typeof(T).ToString() + ")");
                throw;
            }
        }


        public static IEnumerable<string> getEnumerable<T>()
        {
            return Enum.GetNames(typeof(T));
        }
    }
}