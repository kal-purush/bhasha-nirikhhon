using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AudioSteganography
{
  class Program
  {
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern bool Beep(uint dwFreq, uint dwDuration);

    static void Main()
    {
      Beep(15000, 500);
      Console.ReadLine();
    }
  }
}