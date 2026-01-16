using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace robot_stack
{
    class Program
    {/// <summary>
     /// 在二维平面上机器人进行码垛
     /// </summary>
     /// <param name="args"></param>
        static void Main(string[] args)
        {   ///变量初始化
            int row_max = 3;//行数
            int col_max = 4;//列数
            double dx = 90;//码垛间隙，x轴偏移量
            double dy = 110;//码垛间隙，y轴偏移量
            int sc = 0; //码垛计数
            double x_newpos = 0;
            double y_newpos = 0;
            double x_originalpos = 100;  //x，y原始坐标（100，200）
            double y_originalpos = 200;
            Point robot_point = new Point(100, 200);
            Dictionary<int, Point> plink = new Dictionary<int, Point>();
            

            for (int row = 1; row <= row_max+3; row++)  // 遍历行和列，丝车A面 码垛6次，然后翻面，做回清零，丝车B面再码垛6次。
            {
                for (int col = 1; col <= col_max -2; col++)
                {
                    sc++;  //累加码垛次数
                    
                    if (sc > 0 && sc <= 6)    //A面码垛6次
                    {
                        x_newpos = x_originalpos + dx * (col - 1);
                        y_newpos = y_originalpos + dy * (row - 1);   
                        robot_point.x = Convert.ToInt16(x_newpos);   
                        robot_point.y = Convert.ToInt16(y_newpos);
                        Console.WriteLine($"机器人移动A面进行码垛，码垛坐标 ({ robot_point.x},{robot_point.y})");
                    }
                    else if (sc > 6 && sc <= 12) //B面码垛6次
                    {
                        x_newpos = x_originalpos + dx * (col - 1);
                        y_newpos = y_originalpos + dy * (row - 4);
                        robot_point.x = Convert.ToInt16(x_newpos);
                        robot_point.y = Convert.ToInt16(y_newpos);
                        Console.WriteLine($"机器人移动B面进行码垛，码垛坐标 ({ robot_point.x},{robot_point.y})");
                    }
                    if(sc==6)
                    {
                        Console.WriteLine("------------------------------------------------");      
                    }
                }
            }
            Console.ReadLine();
        }
    }
    public class Point
{
    public int x, y;
    public Point(int x, int y)
    {
        this.x = x;
        this.y = y;
    }
}  
}