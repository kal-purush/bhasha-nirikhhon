using System;
using System.Collections.Generic;

namespace Ctlg.Core
{
    public class FileNameComparer : IComparer<File>
    {
        public int Compare(File x, File y)
        {
            if (x == null || x.Name == null)
            {
                if (y == null || y.Name == null)
                {
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            else
            {
                if (y == null || y.Name == null)
                {
                    return 1;
                }
                else
                {
                    return string.Compare(x.Name, y.Name, StringComparison.Ordinal);
                }
            }
        }
    }
}