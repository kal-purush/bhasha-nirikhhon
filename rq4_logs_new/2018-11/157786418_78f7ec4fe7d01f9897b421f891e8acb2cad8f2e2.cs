using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsharpProjWithExt
{
    public static class Extensions
    {
        /// <summary>
        ///  An extension method that references "System.Windows.Forms" assembly.
        /// </summary>
        /// <param name="form"></param>
        /// <returns></returns>
        public static string FormToString(this System.Windows.Forms.Form form)
        {
            return form.ToString();
        }
    }
}