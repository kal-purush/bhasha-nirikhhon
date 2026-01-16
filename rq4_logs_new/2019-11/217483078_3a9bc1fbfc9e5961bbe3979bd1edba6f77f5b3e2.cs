using System;
using System.Collections.Generic;
using System.Text;

namespace Module_5
{
    class Game
    {
        private const int MIN_NUMBERS_OF_POSSIBILITIES = 2;


        public delegate void GameNotificationHandler(string message);
        public event GameNotificationHandler Notification =null;

        public readonly int _trapsNumber = 0;
        public readonly Position _finishPosition = null;

        private int[,] _field = null;
        private int _fieldSize =0;
        private Player _player = null;
        
        public Game(int starPosNumber, int fieldSize, int trapsNumber)
        {
            _fieldSize = fieldSize;
            _trapsNumber = trapsNumber;
            int count = 0;
            Random rnd = new Random();
            _field = new int[_fieldSize, _fieldSize];
            _player = new Player(starPosNumber,_fieldSize);
            _finishPosition = new Position(fieldSize-1-_player._position._x, fieldSize-1-_player._position._y);

            for(int i=0;i<fieldSize;i++)
            {
                for(int j=0;j<fieldSize;j++)
                {
                    Position temp = new Position(i, j);
                    if (count < _trapsNumber && _player._position != temp & _finishPosition != temp)
                    {
                        _field[i, j] = rnd.Next(0, 11);
                        if (_field[i, j] > 0)
                        {
                            count++;
                        }
                    }
                    else
                    {
                        _field[i, j] = 0;
                    }
                }
            }
        }

        public void ConsolePrintField()
        {         
            Notification?.Invoke($"Цель: достич позиции отмеченной как \"fin\"" +
                $"игрок на позиции отмеченной как \"you\", поля, где Вы уже побывали" +
                $"будут помечены как \"sfl\" (от \"safely\")\n");

            Notification?.Invoke("\n");
            if (_field[_player._position._x, _player._position._y] > 0)
            {
                Notification?.Invoke($"Произошел взрыв с силой: " +
                    $"{_field[_player._position._x, _player._position._y]}\n");
                _field[_player._position._x, _player._position._y] = -1;
            }
            Notification?.Invoke($"Колл-во жизней: {_player.Lifes}\n");

            for (int i=0;i<_fieldSize;i++)
            {
                for (int j = 0; j < _fieldSize; j++)
                {

                    if (i == _player._position._x && j == _player._position._y)
                    {
                        Notification?.Invoke("you ");
                        _field[i, j] = -1;
                    }
                    else
                    {
                        if (i == _finishPosition._x && j == _finishPosition._y)
                        {
                            Notification?.Invoke("fin ");
                        }
                        else
                        {
                            if (_field[i, j] == -1)
                            {
                                Notification?.Invoke("sfl ");
                            }
                            else
                            {
                                Notification?.Invoke("___ ");
                            }
                        }
                    }
                }
                Notification?.Invoke("\n");
            }
            Notification?.Invoke("\n");
        }

        public bool Move(ConsoleKeyInfo consoleKeyInfo)
        {
            bool success = false;
            switch(consoleKeyInfo.Key)
            {
                case ConsoleKey.UpArrow:
                    {
                        if(_player._position._x>0)
                        {
                            success = true;
                            _player._position._x -= 1;
                        }
                    }
                    break;
                case ConsoleKey.DownArrow:
                    {
                        if (_player._position._x < _fieldSize-1)
                        {
                            success = true;
                            _player._position._x += 1;
                        }
                    }
                    break;
                case ConsoleKey.LeftArrow:
                    {
                        if (_player._position._y > 0)
                        {
                            success = true;
                            _player._position._y -= 1;
                        }
                    }
                    break;
                case ConsoleKey.RightArrow:
                    {
                        if (_player._position._y < _fieldSize-1)
                        {
                            success = true;
                            _player._position._y += 1;
                        }
                    }
                    break;
                case ConsoleKey.W:
                    {
                        goto case ConsoleKey.UpArrow;
                    }
                case ConsoleKey.S:
                    {
                        goto case ConsoleKey.DownArrow;
                    }
                case ConsoleKey.A:
                    {
                        goto case ConsoleKey.LeftArrow;
                    }
                case ConsoleKey.D:
                    {
                        goto case ConsoleKey.RightArrow;
                    }
            }
            if(_field[_player._position._x, _player._position._y]>0)
            {
                _player.Lifes -= _field[_player._position._x, _player._position._y];
            }
            return success;
        }

        public bool IsGameOver()
        {
            if (_player.Lifes == 0)
            {
                Notification?.Invoke("Вы проиграли");
                return true;
            }
            else
            {
                if (_player._position == _finishPosition)
                {
                    Notification?.Invoke("ВЫ ВЫИГРАЛИ!!!");
                    return true;
                }
                else
                {
                    return false;
                }
            }

        }


    }
}