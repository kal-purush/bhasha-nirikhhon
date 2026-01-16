using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SKSpecial;

namespace TheMathMaze
{
    /// <summary>
    /// 除法也有很多情况…… 只求不要搞事情
    /// </summary>
    class MazeDiv
    {
        public static string get_results(BaseEquation equation)
        {
            string ret = "answer not found\r\n";
            LinkedList<DivEquation> sorted_equation_list = new LinkedList<DivEquation>();
            List<DivEquation> calculated_equation_list = new List<DivEquation>();
            DivEquation root = new DivEquation(equation);
            root.fix();
            sorted_equation_list = sorted_insert(sorted_equation_list, calculated_equation_list, root);
            while (true)
            {
                if (sorted_equation_list.Count == 0)
                {
                    break;
                }
                if (sorted_equation_list.First.Value.ans_found)
                {
                    if (ret == "answer not found\r\n")
                        ret = "";
                    ret += sorted_equation_list.First.Value.equation_console + "\r\n";
                    break;
                }
                LinkedListNode<DivEquation> now = sorted_equation_list.First;
                sorted_equation_list.RemoveFirst();
                List<char> ava_l = now.Value.available_letters_from_last(1);
                List<int> ava_n = now.Value.available_nums();
                for (int i = 0; i < ava_n.Count; i++)
                {
                    if (ava_n[i] == 0)
                    {
                        int first_l = now.Value.get_first_in_each_line().IndexOf(ava_l[0]);
                        if (first_l != -1 && now.Value.spilt_string_without_operator[first_l].Length > 1)
                            continue;
                    }
                    string new_eva = now.Value.replace(ava_l[0], ava_n[i]);
                    DivEquation new_eq = new DivEquation(new_eva);
                    if (new_eq.ans_found)
                    {
                        if (ret == "answer not found\r\n")
                            ret = "";
                        ret += new_eq.equation_console + "\r\n";
                        continue;
                    }
                    sorted_equation_list = sorted_insert(sorted_equation_list, calculated_equation_list, new_eq);
                }
            }
            return ret;
        }

        private static LinkedList<DivEquation> sorted_insert(LinkedList<DivEquation> list, List<DivEquation> calculated, DivEquation to_insert)
        {
            if (to_insert.node_point.compare_to(0) < 0)
                return list;
            if (list.Count == 0)
            {
                list.AddFirst(to_insert);
                calculated.Add(to_insert);
            }
            else
            {
                for (int i = 0; i < calculated.Count; i++)
                {
                    if (to_insert.same_as(calculated[i]))
                        return list;
                }

                LinkedListNode<DivEquation> now = list.First;
                bool inserted = false;
                for (int i = 0; i < list.Count; i++)
                {
                    if (to_insert.node_point.compare_to(now.Value.node_point) < 0)
                    {
                        list.AddBefore(now, to_insert);
                        calculated.Add(to_insert);
                        inserted = true;
                        break;
                    }
                    now = now.Next;
                }
                if (!inserted)
                {
                    list.AddLast(to_insert);
                    calculated.Add(to_insert);
                }
            }
            return list;
        }
    }
    class DivEquation : BaseEquation
    {
        public SKSpecialDecimal node_point
        {
            get
            {
                return cal_node_point();
            }
        }
        /// <summary>
        /// 找到结果了？
        /// </summary>
        public bool ans_found
        {
            get
            {
                return (node_point.is_zero());
            }
        }
        public DivEquation(string str) : base(str)
        {
            if (method != METHOD.DIV)
            {
                clear();
            }
            init();
        }
        public DivEquation(BaseEquation _be) : base(_be.equation_console)
        {
            if (method != METHOD.DIV)
            {
                clear();
            }
            init();
        }

        private void init()
        {
            cal_node_point();
        }
        private SKSpecialDecimal cal_node_point()
        {
            if (equation_console == "")
                return new SKSpecialDecimal();
            if (NodePointStatic.compare_to(new SKSpecialDecimal(-1)) != 0)
                return NodePointStatic;

            SKSpecialDecimal ret = new SKSpecialDecimal(0);
            //剪枝0：行数
            string[] lines = spilt_string;
            if (lines.Length < 5)
            {
                ret = new SKSpecialDecimal(-2);
                NodePointStatic = ret;
                return ret;
            }
            int dif = lines[0].Length - lines[1].Length + 1;
            int dif_p = (dif < 0) ? 0 : dif;
            if (lines.Length != 3 + lines[lines.Length - 1].Length * 2)//样例：768/9和768/14
            {
                ret = new SKSpecialDecimal(-2);
                NodePointStatic = ret;
                return ret;
            }
            //剪枝1:结果位数太多
            if (lines[lines.Length - 1].Length > dif_p + 1)
            {
                ret = new SKSpecialDecimal(-2);
                NodePointStatic = ret;
                return ret;
            }
            ////剪枝1.5:检查按行掉落
            //for (int i = 0; i < lines[lines.Length - 1].Length - 1; i++)
            //{
            //    if (lines[2 * i + 4][lines[2 * i + 4].Length - 1] != lines[0][lines[1].Length - 1 + i])
            //    {
            //        ret = new SKSpecialDecimal(-2);
            //        NodePointStatic = ret;
            //        return ret;
            //    }
            //}
            //剪枝2:乘法
            string be_div_last_num = get_last_num(lines[1]);
            if (be_div_last_num.Length > 0)
            {
                SKSpecialDecimal be_div_last_num_num = new SKSpecialDecimal(be_div_last_num);
                for (int i = 0; i < lines[lines.Length - 1].Length; i++)
                {
                    char c = lines[lines.Length - 1][i];
                    if (c >= '0' && c <= '9')
                    {
                        string right_num = get_last_num(lines[2 * i + 2]);
                        int min_last_len = Math.Min(right_num.Length, be_div_last_num.Length);
                        SKSpecialDecimal left_num = be_div_last_num_num * (int)(c - '0');
                        string left = left_num.to_string_only_integer();
                        if (left.Length < min_last_len)
                        {
                            string tmpstring = "";
                            for (int j = 0; j < min_last_len - left.Length; j++)
                                tmpstring += "0";
                            left = tmpstring + left;
                        }
                        left = left.Substring(left.Length - min_last_len);
                        string right = right_num.Substring(right_num.Length - min_last_len);
                        if (left != right)
                        {
                            ret = new SKSpecialDecimal(-2);
                            NodePointStatic = ret;
                            return ret;
                        }
                    }

                }
            }
            //TODO:剪枝3：检查减法

            //计算分值返回
            List<int> ava = available_nums();
            if (ava.Count == 0)
                return new SKSpecialDecimal(0);
            int avg = (int)Math.Round((double)ava.Sum() / ava.Count);
            if (avg >= 10)
                avg = 9;
            else if (avg <= 0)
                avg = 0;
            string tmpb = equation_console;
            for (int i = 0; i < tmpb.Length; i++)
                if (tmpb[i] >= 'A' && tmpb[i] <= 'J')
                    tmpb = tmpb.Replace(tmpb[i], (char)(avg + '0'));
            string[] _lines = tmpb.Split(new char[1] { '_' });
            _lines[1] = _lines[1].Substring(1);
            //1:乘法差异
            SKSpecialDecimal be_div = new SKSpecialDecimal(_lines[1]);
            for (int i = 0; i < _lines[_lines.Length - 1].Length; i++)
            {
                ret += SKSpecialDecimal.abs(be_div * (_lines[_lines.Length - 1][i] - '0') - (new SKSpecialDecimal(_lines[2 * i + 2])));
            }
            //2:减法差异


            if (_lines[_lines.Length - 1].Length == 1)
            {
                //2.0:第一减法差异
                SKSpecialDecimal b1, b2, b3;
                b2 = new SKSpecialDecimal(_lines[2]);
                b3 = new SKSpecialDecimal(_lines[3].Substring(0, _lines[3].Length));
                int fetch = _lines[0].Length - _lines[_lines.Length - 1].Length + 1;
                b1 = new SKSpecialDecimal(_lines[0].Substring(0, fetch));
                ret += SKSpecialDecimal.abs((b1 - b2) - b3);
            }
            else
            {
                //2.0:第一减法差异
                SKSpecialDecimal b1, b2, b3;
                b2 = new SKSpecialDecimal(_lines[2]);
                if (_lines[3].Length == 1)
                    b3 = new SKSpecialDecimal(0);
                else
                    b3 = new SKSpecialDecimal(_lines[3].Substring(0, _lines[3].Length - 1));
                int fetch = _lines[0].Length - _lines[_lines.Length - 1].Length + 1;
                b1 = new SKSpecialDecimal(_lines[0].Substring(0, fetch));
                ret += SKSpecialDecimal.abs((b1 - b2) - b3);
                //2.1:最后减法差异
                SKSpecialDecimal c1, c2, c3;
                c1 = new SKSpecialDecimal(_lines[_lines.Length - 4]);
                c2 = new SKSpecialDecimal(_lines[_lines.Length - 3]);
                c3 = new SKSpecialDecimal(_lines[_lines.Length - 2]);
                ret += SKSpecialDecimal.abs((c1 - c2) - c3);
                //2.2:其他减法差异
                for (int i = 0; i < (lines.Length - 3) / 2 - 2; i++)
                {
                    SKSpecialDecimal a1 = new SKSpecialDecimal(_lines[3 + i * 2]);
                    SKSpecialDecimal a2 = new SKSpecialDecimal(_lines[4 + i * 2]);
                    SKSpecialDecimal a3;
                    if (_lines[5 + i * 2].Length == 1)
                        a3 = new SKSpecialDecimal(0);
                    else
                        a3 = new SKSpecialDecimal(_lines[5 + i * 2].Substring(0, _lines[5 + 3 * i].Length - 1));
                    ret += SKSpecialDecimal.abs((a1 - a2) - a3);
                }
                ret = SKSpecialDecimal.abs(ret) + available_letters().Count;
                if (!ret.is_zero() && available_letters().Count == 0)
                {
                    ret = new SKSpecialDecimal(-2);
                    NodePointStatic = ret;
                    return ret;
                }
                NodePointStatic = ret;
                return ret;
            }
            return ret;
        }
        /// <summary>
        /// 专门处理被除数明显大于除数的情况
        /// </summary>
        public void fix()
        {
            if (node_point.compare_to(0) >= 0)
            {
                string[] lines = spilt_string;
                int dif = lines[0].Length - lines[1].Length + 1;
                if (dif < 0)
                {
                    replace(lines[0][0], 0);
                }
            }
        }
        private SKSpecialDecimal NodePointStatic = new SKSpecialDecimal(-1);

        private string get_last_num(string line)
        {
            string ret = "";
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[line.Length - i - 1];
                if (c >= '0' && c <= '9')
                {
                    ret = ret.Insert(0, c.ToString());
                }
                else
                {
                    break;
                }
            }
            return ret;
        }
    }
}