using HelloDotNetGuide.异步编程;
            #region 异步编程

            //var getFileContent = ReadFileAsyncExample.ReadFileAsync("D:\\Desktop\\数据读取.txt").ConfigureAwait(false);

            #endregion

            //递归算法.FibonacciSum();
            //递归算法.RecursiveAlgorithmSum();
﻿namespace HelloDotNetGuide.常见算法
{
    public class 递归算法
    {
        #region 使用C#语言编写的递归算法来计算1+2+3+4+…+100的结果

        /// <summary>
        /// 使用C#语言编写的递归算法来计算1+2+3+4+…+100的结果
        /// 最终结果是：5050
        /// </summary>
        public static void RecursiveAlgorithmSum()
        {
            int result = SumNumbers(100);
            Console.WriteLine("1+2+3+4+...+100 = " + result);
        }

        public static int SumNumbers(int n)
        {
            if (n == 1)
            {
                return 1;//递归结束条件
            }
            else
            {
                return n + SumNumbers(n - 1);
            }
        }

        #endregion

        #region 一列数的规则如下 : 1 、 1 、 2 、 3 、 5 、 8 、 13 、 21 、 34… 求第 30 位数是多少， 用递归算法实现

        /// <summary>
        /// 使用递归算法来实现求解斐波纳契数列中第30位数的值
        /// 最终结果为832040
        /// </summary>
        public static void FibonacciSum()
        {
            int result = Fibonacci(30);
            Console.WriteLine("第30位斐波那契数是：" + result);
        }

        public static int Fibonacci(int n)
        {
            if (n <= 2)
            {
                return 1;
            }
            else
            {
                return Fibonacci(n - 1) + Fibonacci(n - 2);
            }
        }

        #endregion
    }
}
﻿namespace HelloDotNetGuide.异步编程
{
    /// <summary>
    /// 使用C#异步方法来进行文件内容读取操作
    /// </summary>
    public class ReadFileAsyncExample
    {
        /// <summary>
        /// 异步方法读取文件内容
        /// 当涉及到C#的异步编程时，你可以使用 async 和 await 关键字来实现
        /// </summary>
        /// <param name="filePath">文件地址</param>
        /// <returns></returns>
        public static async Task<string> ReadFileAsync(string filePath)
        {
            try
            {
                using (StreamReader reader = new StreamReader(filePath))
                {
                    // 异步读取文件内容并等待完成
                    string content = await reader.ReadToEndAsync();
                    return content;
                }
            }
            catch (FileNotFoundException)
            {
                return "文件未找到";
            }
            catch (Exception ex)
            {
                return $"发生错误：{ex.Message}";
            }
        }
    }
}