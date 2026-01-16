using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Numerics;

using CircuitCalculation.Elements;

namespace CircuitCalculation
{
    /// <summary>
    /// Цепь элементов
    /// </summary>
    public interface ICircuit
    {
        /// <summary>
        /// Элементы цепи
        /// </summary>
        List<IElement> Elements { get; set; }

        /// <summary>
        /// Родительская цепь
        /// </summary>
        ICircuit ParentCircuit { get; set; }

        /// <summary>
        /// Соединения, входящие в текущую цепь
        /// </summary>
        List<ICircuit> SubCircuits { get; set; }

        /// <summary>
        /// Расчитать импеданс цепи на заданных частотах
        /// </summary>
        /// <param name="frequencies">Список частот</param>
        /// <returns>Расчитанное значение импеданса</returns>
        Complex[] CalculateZ(double[] frequencies);

        /// <summary>
        /// Добавить элемент по указанному индексу
        /// </summary>
        /// <param name="element">Элемент схемы</param>
        /// <param name="index">Индекс</param>
        void AddElement(IElement element, int index);

        /// <summary>
        /// Удалить элемент по заданному индексу
        /// </summary>
        /// <param name="index">Индекс</param>
        void RemoveElement(int index);


        /// <summary>
        /// Зажигается при измененииях в цепи
        /// </summary>
        event EventHandler CircuitChanged;
    }
}