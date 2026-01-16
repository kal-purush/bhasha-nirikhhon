using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Drawing;
using System.Threading.Tasks;

namespace AvitoRuslanParser
{
  class LogMessageColor
  {
    public static Color Information()
    {
      return Color.Black;
    }
    public static Color Warning()
    {
      return Color.Gold;
    }
    public static Color Error()
    {
      return Color.Red;
    }
  }
}