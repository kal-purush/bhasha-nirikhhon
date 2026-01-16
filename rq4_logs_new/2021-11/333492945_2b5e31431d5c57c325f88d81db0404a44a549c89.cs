using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebRPC.Models
{
    public interface ITestService
    {
        void Simple();
        void Simple(bool b);
        void Simple(byte b);
        void Simple(short s);
        void Simple(int i);
        void Simple(long l);
        void Simple(decimal d);
        void Simple(float f);
        void Simple(double d);
        void Simple(DateTime d);
        void Simple(string s);

        bool SimpleReturn(bool b);
        byte SimpleReturn(byte b);
        short SimpleReturn(short s);
        int SimpleReturn(int i);
        long SimpleReturn(long l);
        decimal SimpleReturn(decimal d);
        float SimpleReturn(float f);
        double SimpleReturn(double d);
        DateTime SimpleReturn(DateTime d);
        string SimpleReturn(string s);

        void SimpleRef(ref bool b);
        void SimpleRef(ref byte b);
        void SimpleRef(ref short s);
        void SimpleRef(ref int i);
        void SimpleRef(ref long l);
        void SimpleRef(ref decimal d);
        void SimpleRef(ref float f);
        void SimpleRef(ref double d);
        void SimpleRef(ref DateTime d);
        void SimpleRef(ref string s);

        void SimpleOut(out bool b);
        void SimpleOut(out byte b);
        void SimpleOut(out short s);
        void SimpleOut(out int i);
        void SimpleOut(out long l);
        void SimpleOut(out decimal d);
        void SimpleOut(out float f);
        void SimpleOut(out double d);
        void SimpleOut(out DateTime d);
        void SimpleOut(out string s);
    }
}