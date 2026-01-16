using System;
using System.Collections.Generic;
using System.Linq;

namespace Sample.ConsoleApp
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var model = new SampleModel();
            model.Arguments = args.ToList();

            Console.WriteLine("Hello World!" + string.Join(":::", model.Arguments));
        }
    }

    public class SampleModel
    {
        public List<string> Arguments { get; set; } = new List<string>();
        /* 
         * List / Collection 등은 위와 같이 초기화하는 방법을 추천.
         * public List<string> Arguments { get; set; } 만 한 경우 null로 들어감. 
         * 널체크를 하는게 약간의 코드스멜일 수 있다
         */
    }
}