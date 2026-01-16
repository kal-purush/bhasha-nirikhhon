using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    /// <summary>
    /// 矩形类(子类):形状
    /// 也是正方形的父类
    /// </summary>
    class Rectangular:Shape
    {
        /// <summary>
        /// 长度
        /// </summary>
        protected double Length;

        /// <summary>
        /// 宽度
        /// </summary>
        protected double Weight;

        /// <summary>
        /// 无参的构造函数
        /// </summary>
        public Rectangular()
        {

        }
    
        /// <summary>
        /// 矩形的有参构造函数
        /// 初始化颜色,长度,宽度
        /// </summary>
        /// <param name="Color">颜色</param>
        /// <param name="Length">长度</param>
        /// <param name="Weight">宽度</param>
        public Rectangular(string Color,double Length,double Weight)
        {
            this.Color = Color;
            this.Length = Length;
            this.Weight = Weight;

        }

        /// <summary>
        /// 获取矩形面积
        /// </summary>
        /// <returns>返回面积</returns>
        public double GetArea()
        {
            return Length * Weight;
        }

        /// <summary>
        /// 获取矩形周长
        /// </summary>
        /// <returns>返回周长</returns>
        public double GetPerimeter()
        {
            return (Length + Weight) * 2;
        }
    }
}