using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TheMathMazeWindow
{
    /// <summary>
    /// 用于GUI模式和Console模式的互换
    /// </summary>
    class ExpressionTranslate
    {
        public static string get_GUI(TheMathMaze.BaseEquation be)
        {
            string ret = "";
            string[] lines = be.spilt_string_without_operator;
            if (be.method == TheMathMaze.BaseEquation.METHOD.ADD || be.method == TheMathMaze.BaseEquation.METHOD.SUB)
            {
                ret += lines[0];
                ret += "\r\n";
                ret += (be.method == TheMathMaze.BaseEquation.METHOD.ADD) ? "+ " : "- ";
                if (lines[1].Length < lines[0].Length)
                {
                    for (int i = 0; i < lines[0].Length - lines[1].Length; i++)
                    {
                        ret += " ";
                    }
                }
                ret += lines[1];
                ret += "\r\n";
                for (int i = 0; i < Math.Max(lines[0].Length, Math.Max(lines[1].Length, lines[2].Length)); i++)
                {
                    ret += "—";
                }
                ret += "\r\n";
                ret += lines[2];
            }
            if (be.method == TheMathMaze.BaseEquation.METHOD.MUL)
            {

            }
            return ret;
        }
    }
}