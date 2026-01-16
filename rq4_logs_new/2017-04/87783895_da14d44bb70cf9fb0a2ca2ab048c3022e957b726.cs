using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Drawing;
using System.IO;
namespace BHXH.Util
{
    public class Image
    {
        public static System.Drawing.Image BinaryToImage(System.Data.Linq.Binary binary)
        {
            if (binary.Length > 0)
            {
                byte[] b = binary.ToArray();
                MemoryStream ms = new MemoryStream(b);
                System.Drawing.Image img = System.Drawing.Image.FromStream(ms);
                return img;
            }
            else return null;
        }
    }
}