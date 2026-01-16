using CSharpSample.DesignPattern.Factory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpSample.SampleCode
{
    class DelegateSample : ISamplePractitioner
    {
        public bool Do(int exampleNo)
        {
            switch (exampleNo)
            {
                case 1:
                    FuncSample01();
                    break;
                default:
                    break;
            }

            return true;
        }

        /// <summary>
        /// Func(int, int) 引数int, 戻り値intを持つデリゲートのサンプル
        /// </summary>
        private void FuncSample01()
        {
            int seed = 3;

            // デリゲート(Func01)の利用例01
            // デリゲート(Func01)を実行
            var value01 = Func01(seed);
            Console.WriteLine($"value : {value01}");

            // デリゲート(Func01)の利用例02
            // デリゲート(Func01)をメソッドに渡して利用する
            var value02 = MethodWithFunc(seed, func01: Func01);
            Console.WriteLine($"value : {value02}");

            // デリゲート(Func01)の利用例03
            // デリゲート(Func01)をnullでメソッドに渡すと動作しない
            var value03 = MethodWithFunc(seed);
            Console.WriteLine($"value : {value03}");
        }

        private int MethodWithFunc(int value, Func<int, int> func01 = null)
        {
            int answer = 0;

            if (func01 != null)
            {
                answer = func01(value);
            }

            return answer;
        }

        /// <summary>
        /// 引数int,戻り値intを持つデリゲート
        /// </summary>
        Func<int, int> Func01 = (x) =>
        {
            return x * 2;
        };
    }
}