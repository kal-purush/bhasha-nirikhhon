using System;

public class FormattedRow : StringRow
{
    public FormattedRow(string value) : base(value)
    {
    }

    public int GetWordCount()
    {
        return Value.Split(new[] { ' ', '\t', '\n' }, StringSplitOptions.RemoveEmptyEntries).Length;
    }

    public void PrintInfo()
    {
        Console.WriteLine($"Рядок: {Value}");
        Console.WriteLine($"Довжина: {GetLength()}");
        Console.WriteLine($"Кількість слів: {GetWordCount()}");
    }
}