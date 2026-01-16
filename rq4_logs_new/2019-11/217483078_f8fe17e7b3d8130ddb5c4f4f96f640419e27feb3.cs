using System;
using System.Collections.Generic;
using System.Text;

namespace Module_5
{
    class Game
    {
        // Версия БЕЗ класса Player
        private const int MIN_NUMBERS_OF_POSSIBILITIES = 2;

        public delegate void GameHandler(string message);
        public event GameHandler Notification =null;

        private int _trapsNumber = 0;
        private PositionOnThePlane _finishPosition = null;

        private int[,] _field = null;
        private int _fieldSize =0;
        private PositionOnThePlane _playerPosition = null;
        private int _playerLifes = 10;
        
        public Game(int starPosNumber, int fieldSize, int trapsNumber)
        {
            _fieldSize = fieldSize;
            _trapsNumber = trapsNumber;
            int count = 0;
            Random rnd = new Random();
            _field = new int[_fieldSize, _fieldSize];

            switch (starPosNumber)
            {
                case 1:
                    {
                        _playerPosition = new PositionOnThePlane(0, 0);
                    }
                    break;
                case 2:
                    {
                        _playerPosition = new PositionOnThePlane(0, fieldSize - 1);
                    }
                    break;
                case 3:
                    {
                        _playerPosition = new PositionOnThePlane(fieldSize - 1, 0);
                    }
                    break;
                case 4:
                    {
                        _playerPosition = new PositionOnThePlane(fieldSize - 1, fieldSize - 1);
                    }
                    break;
            }

            _finishPosition = new PositionOnThePlane(fieldSize-1- _playerPosition.X, fieldSize-1- _playerPosition.Y);

            for(int i=0;i<fieldSize;i++)
            {
                for(int j=0;j<fieldSize;j++)
                {
                    PositionOnThePlane temp = new PositionOnThePlane(i, j);
                    if (count < _trapsNumber && _playerPosition != temp & _finishPosition != temp)
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
            Notification?.Invoke($"Цель: достич позиции отмеченной как \"fin\", " +
                $"игрок на позиции отмеченной как \"you\", поля, где Вы уже побывали " +
                $"будут помечены как \"sfl\" (от \"safely\"), " +
                $"на стартовой и финишной клетках - безопастно\n" +
               $"Осталось {_trapsNumber} рабочих ловушек.\n");

            Notification?.Invoke("\n");
            if (_field[_playerPosition.X, _playerPosition.Y] > 0)
            {
                Notification?.Invoke($"Произошел взрыв с силой: " +
                    $"{_field[_playerPosition.X, _playerPosition.Y]}\n");
                _field[_playerPosition.X, _playerPosition.Y] = -1;
            }
            Notification?.Invoke($"Колл-во жизней: {_playerLifes}\n");

            for (int i=0;i<_fieldSize;i++)
            {
                for (int j = 0; j < _fieldSize; j++)
                {

                    if (i == _playerPosition.X && j == _playerPosition.Y)
                    {
                        Notification?.Invoke("you ");
                        _field[i, j] = -1;
                    }
                    else
                    {
                        if (i == _finishPosition.X && j == _finishPosition.Y)
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
                        if(_playerPosition.X > 0)
                        {
                            success = true;
                            _playerPosition.X -= 1;
                        }
                    }
                    break;
                case ConsoleKey.DownArrow:
                    {
                        if (_playerPosition.X < _fieldSize-1)
                        {
                            success = true;
                            _playerPosition.X += 1;
                        }
                    }
                    break;
                case ConsoleKey.LeftArrow:
                    {
                        if (_playerPosition.Y > 0)
                        {
                            success = true;
                            _playerPosition.Y -= 1;
                        }
                    }
                    break;
                case ConsoleKey.RightArrow:
                    {
                        if (_playerPosition.Y < _fieldSize-1)
                        {
                            success = true;
                            _playerPosition.Y += 1;
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
            if(_field[_playerPosition.X, _playerPosition.Y] >0)
            {
                _playerLifes -= _field[_playerPosition.X, _playerPosition.Y];
                _trapsNumber--;
            }
            return success;
        }

        public bool IsGameOver()
        {
            if (_playerLifes > 0 && _playerPosition != _finishPosition)
            {
                return false;
            }
            else
            {
                ConsolePrintField();
                if (_playerLifes <= 0)
                {
                    Notification?.Invoke("\nВы проиграли\n");
                }
                else
                {
                    Notification?.Invoke("\nВЫ ВЫИГРАЛИ!!!\n");                    
                }
                return true;
            }

        }


    }
}