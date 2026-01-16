using System.Collections.Generic;

namespace FizzBuzz.ConsoleApp
{
    public interface IFizzBuzzService
    {
        string GetFizzBuzzValue(int i);
        List<string> GetFizzBuzzResults(int numberOfItems);
    }
}