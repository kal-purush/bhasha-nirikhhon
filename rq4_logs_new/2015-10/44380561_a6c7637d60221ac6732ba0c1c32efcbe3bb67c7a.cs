using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace UnitTests
{
    [TestClass]
    public class CursorAddressTests
    {
        [TestCategory( "Data Format" ), TestCategory( "Data Format\\Cursor" ), TestMethod]
        public void CursorAddress70022()
        {
            string path = @"D:\Clients\7.0.2.2";
            CursorTest( path );
        }

        [TestCategory( "Data Format" ), TestCategory( "Data Format\\Cursor" ), TestMethod]
        public void CursorAddress70200()
        {
            string path = @"D:\Clients\7.0.20.0";
            CursorTest( path );
        }

        [TestCategory( "Data Format" ), TestCategory( "Data Format\\Cursor" ), TestMethod]
        public void CursorAddress70351()
        {
            string path = @"D:\Clients\7.0.35.1";
            CursorTest( path );
        }

        [TestCategory( "Data Format" ), TestCategory( "Data Format\\Cursor" ), TestMethod]
        public void CursorAddress70450()
        {
            string path = @"D:\Clients\7.0.45.0";
            CursorTest( path );
        }

        [TestCategory( "Data Format" ), TestCategory( "Data Format\\Cursor" ), TestMethod]
        public void CursorAddress70462()
        {
            string path = @"D:\Clients\7.0.46.2";
            CursorTest( path );
        }

        public void CursorTest(string path)
        {
            byte[] fileBytes = File.ReadAllBytes( Path.Combine( path, "client.exe" ) );

            byte[] sig = new byte[] { 0x8B, 0xF1, 0x83, 0xBE, 0xFC, 0x00, 0x00, 0x00, 0x00 };
            int offset = 0;
            bool found = FindSignatureOffset( sig, fileBytes, out offset );
            Assert.IsTrue( found, "Signature not found." );

            int address = 0;

            if (found)
            {
                offset += sig.Length;
                for (int i = 0; i < 10; i++)
                {
                    if (fileBytes[offset + i] == 0xA1)
                    {
                        address = BitConverter.ToInt32( fileBytes, ( offset + i ) + 1 );
                        break;
                    }
                }
            }
            Assert.IsNotNull( address );
        }

        public static bool FindSignatureOffset( byte[] signature, byte[] buffer, out int offset )
        {
            bool found = false;
            offset = 0;
            for (int x = 0; x < buffer.Length - signature.Length; x++)
            {
                for (int y = 0; y < signature.Length; y++)
                {
                    if (signature[y] == 0xCC || buffer[x + y] == signature[y])
                        found = true;
                    else
                    {
                        found = false;
                        break;
                    }
                }
                if (found)
                {
                    offset = x;
                    break;
                }
            }
            return found;
        }

    }
}