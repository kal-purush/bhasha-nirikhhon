using System;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Collections.Generic;

namespace HTTPServerProject.ReadStreams
{
    public interface IReadStreams
    {
        int Read();
        string ReadLine();
        int Peek();
        void Close();
    }

    public class MyStreamReader : IReadStreams
    {
        Stream rStream = new MemoryStream();
        StreamReader reader = default!;

        public MyStreamReader(Stream stream)
        {
            rStream = stream;
            reader = new StreamReader(stream);
        }

        public string ReadLine()
        {
            string input = reader.ReadLine()!;
            return input;
        }

        public int Read()
        {
            var input = reader.Read();
            return input;
        }

        public int Peek()
        {
            var input = reader.Peek();
            return input;
        }

        public void Close()
        {
            reader.Close();
        }
    }

    public class TestStreamReader : IReadStreams
    {
        List<string> sArr = default!;

        public TestStreamReader(List<string> arr)
        {
            sArr = arr;
        }

        public string ReadLine()
        {
            string input = sArr[0];
            sArr.RemoveAt(0);
            return input;
        }

        public int Read()
        {
            var input = sArr[0];
            if (input == "")
            {
                return -1;
            }
            else
            {
                var c = input[0];
                var ascii_c = (int)c;

                if (input.Length > 1)
                {
                    sArr[0] = input.Substring(1);
                }
                else
                {
                    sArr[0] = "";
                }

                return ascii_c;
            }
        }

        public int Peek()
        {
            var input = sArr[0];
            if (input == "")
            {
                return -1;
            }
            else
            {
                var c = input[0];
                var ascii_c = (int)c;

                return ascii_c;
            }
        }

        public void Close()
        {

        }
    }


}