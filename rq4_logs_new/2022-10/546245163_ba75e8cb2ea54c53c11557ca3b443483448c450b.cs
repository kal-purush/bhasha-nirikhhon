using System;
using System.Net;
using System.Net.Sockets;
using System.IO;

namespace HTTPServerProject.Interfaces
{
    public interface INewStreamReader
    {
        string ReadLine();
        void Close();
    }
    public abstract class NewStreamReader : INewStreamReader
    {
        Stream emptyStream = null!;
        StreamReader reader = null!;
        List<string> emptyArr = null!;
        
        public NewStreamReader(Stream stream = null!, List<string> arr = null!)
        {
            stream = emptyStream;
            reader = new StreamReader(stream);
            arr = emptyArr;
        }

        public abstract string ReadLine();

        public abstract void Close();
    }

    public class MyStreamReader : NewStreamReader
    {
        Stream emptyStream = null!;
        StreamReader reader = null!;
        List<string> emptyArr = null!;
        public MyStreamReader(Stream stream = null!, List<string> arr = null!)
        {
            stream = emptyStream;
            reader = new StreamReader(stream);
            arr = emptyArr;
        }
        public override string ReadLine()
        {
            string input = reader.ReadLine()!;
            return input;
        }

        public override void Close()
        {
            reader.Close();
        }
    }


    public class TestStreamReader
    {
        Stream emptyStream = null!;
        StreamReader reader = null!;
        string[]? emptyArr = null;
        public TestStreamReader(Stream stream = null!, List<string> arr = null!)
        {
            stream = emptyStream;
            reader = new StreamReader(stream);

            if (arr == null)
            {
                arr = new List<string>();
            }
        }
        public string ReadLine()
        {
            string input = arr[0];
            arr.RemoveAt(0);
            return input;
        }

        public void Close()
        {

        }
    }
    


}