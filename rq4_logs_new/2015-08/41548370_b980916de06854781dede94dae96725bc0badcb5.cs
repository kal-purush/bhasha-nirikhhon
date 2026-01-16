using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Products
{
    public class BoxSize : Size
    {
        public BoxSize(int l, int w, int h)
        {
            L = l;
            W = w;
            H = h;
        }
        public BoxSize(int[] size)
        {
            L = size[0];
            W = size[1];
            H = size[2];
        }

        public BoxSize(BoxSize box)
        {
            L = box.L;
            W = box.W;
            H = box.H;
        }
        
        public int L { get; set; }
        public int W { get; set; }
        public int H { get; set; }

        public override string GetAsText()
        {
            return String.Format("{0}x{1}x{2} mm", L, W, H);
        }
    }       
}