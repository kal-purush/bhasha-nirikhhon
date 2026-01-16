using System;
using System.Numerics;
using System.Diagnostics;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FStream = System.IO.FileStream;

// If using Dafny 2.2.0, use @__default; if using Dafny 2.3.0, use _module
//namespace @__default {
namespace _module {

    public partial class HostConstants
    {
        public static uint NumCommandLineArgs()
        {
            return (uint)System.Environment.GetCommandLineArgs().Length;
        }

        public static char[] GetCommandLineArg(ulong i)
        {
            return System.Environment.GetCommandLineArgs()[i].ToCharArray();
        }
    }

    public partial class FileStream
    {
        internal FStream fstream;
        internal FileStream(FStream fstream) { this.fstream = fstream; }

        public static bool FileExists(char[] name)
        {
        return System.IO.File.Exists(new string(name));      
        }

        public static void FileLength(char[] name, out bool success, out int len)
        {
        len = 42;
        try
        {
            System.IO.FileInfo fi = new System.IO.FileInfo(new string(name));
            if (fi.Length < System.Int32.MaxValue)  // We only support small files
            {
            len = (int)fi.Length;
            success = true;
            }
            else
            {
            success = false;
            }
            
        }
        catch (Exception e)
        {
            System.Console.Error.WriteLine(e);
            success = false;
        }     
        }

        public static void Open(char[] name, out bool ok, out FileStream f)
        {
            try
            {
                f = new FileStream(new FStream(new string(name), System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite));
                ok = true;
            }
            catch (Exception e)
            {
                System.Console.Error.WriteLine(e);
                f = null;
                ok = false;
            }
        }

        public bool Close()
        {
	    bool ok;
            try
            {
                fstream.Close();
                ok = true;
            }
            catch (Exception e)
            {
                System.Console.Error.WriteLine(e);
                ok = false;
            }
	    return ok;
        }

        public bool Read(int file_offset, byte[] buffer, int start, int num_bytes)
        {
	    bool ok;
            try
            {
                fstream.Seek(file_offset, System.IO.SeekOrigin.Begin);
                fstream.Read(buffer, start, num_bytes);
                ok = true;
            }
            catch (Exception e)
            {
                System.Console.Error.WriteLine(e);
                ok = false;
            }
	    return ok;
        }

        public bool Write(int file_offset, byte[] buffer, int start, int num_bytes)
        {
	    bool ok;
            try
            {
                fstream.Seek(file_offset, System.IO.SeekOrigin.Begin);
                fstream.Write(buffer, start, num_bytes);
                ok = true;
            }
            catch (Exception e)
            {
                System.Console.Error.WriteLine(e);
                ok = false;
            }
	    return ok;
        }
    }

}