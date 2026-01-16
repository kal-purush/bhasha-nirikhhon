using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SeaBattleFramework
{
    public enum ShipType
    {
        SingleDecker = 1,
        TwoDecker,
        ThreeDecker,
        FourDecker
    }

    public enum ShipStatus
    {
        Full,
        Damaged,
        Destroyed
    }

    public class Ship
    {
        public int Id { get; set; }
        public string Name { get; set; } = "UnknownShip";               // TODO: добавить список имён и генерировать случайно из него
        public ShipType Type { get; set; }
        public ShipStatus Status { get; set; } = ShipStatus.Full;
        public Point[] Position { get; set; }

        public int Health { get; set; }

        public ShipStatus Injury()
        {
            return Status = (--Health) <= 0 ? ShipStatus.Destroyed : ShipStatus.Damaged;
        }

        /// <summary>
        /// Инициализирует новый экземпляр класса <c>Ship</c> с указанным типом и расположением на поле
        /// </summary>
        /// <param name="type">Тип корабля</param>
        /// <param name="pos">Отсортированный список позиций. 
        /// Первый элемент - позиция головы корбля, последний элмент - позиция хвоста</param>
        public Ship(ShipType type, List<Point> pos)
        {
            Type = type;

            Position = new Point[(int)Type];
            for (int i = 0; i < (int)Type; i++)
                Position[i] = pos[i];

            Id = Position[0].X * 10 + Position[0].Y;

            Health = (int)Type;
        }
    }
}