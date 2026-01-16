using RedisCacheTest.Core.Helpers;
using RedisCacheTest.Core.UIDatas;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    class Program
    {
        static void Main(string[] args)
        {
            RedisConnectHelper.initial();
            ChatRoomUIData ChatRoomUIData = new ChatRoomUIData();
            ChatRoomUIData.subscrite();
            Console.WriteLine("請輸入名字 : ");
            string shit = Console.ReadLine();
            Console.WriteLine("請輸入訊息 : ");
            ChatRoomUIData.addComment(shit, "SamSamSam", Console.ReadLine());
            //監聽
            Console.ReadLine();
        }


    }
}