using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Impower.Office365
{
    public static class LinqExtensions
    {
        public static IEnumerable<T> TakeUntilPlus<T>(this IEnumerable<T> list, Func<T, bool> predicate, int plus)
        {
            bool found = false;
            int counter = 0;
            foreach (T el in list)
            {
                if (predicate(el) && !found)
                {
                    found = true;
                }
                else if(counter >= plus)
                {
                    yield break;
                }
                else if(found)
                {
                    counter++;
                }
                yield return el;
            }
        }
    }
}