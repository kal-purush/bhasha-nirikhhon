using System;
using System.IO;
using System.Text;

namespace Chakra
{
  public class VirtualStdOut : TextWriter, IDisposable
  {
    public TextWriter Captured { get;}
    public override Encoding Encoding { get { return Encoding.ASCII; } }

    public VirtualStdOut()
    {
      Captured = new StringWriter();
    }
    
    override public void Write(string output)
    {
      Captured.Write(output);
    }

    override public void WriteLine(string output)
    {
      Captured.WriteLine(output);
    }
  }
}