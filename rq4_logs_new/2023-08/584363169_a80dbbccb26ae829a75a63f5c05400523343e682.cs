// See https://aka.ms/new-console-template for more information
using Tetris;

internal class FigureGenerator
{
    private int _x;     //_x - нижняя черточка по правилам хорошего кода отмечает, что это переменная c модификатором доступа private
    private int _y;
    private char _c;

    private Random _rand = new Random();

    public FigureGenerator(int x, int y, char c)
    {
        _x = x;     //по-другому можно использовать указатель "this._x = x"
        _y = y;
        _c = c;
    }

    public Figure GetNewFigure()
    {
        if (_rand.Next(0, 2) == 0)
            return new Square(_x, _y, _c);
        else
            return new Stick(_x, _y, _c);
    }
}