using System;

namespace Exercism.Exceptions
{
  public static class SimpleCalculator
  {
    public static string Calculate(int operand1, int operand2, string operation)
    {
      string answer = $"{operand1} {operation} {operand2} = ";
      try
      {
        int calculated = operation switch
        {
          "+" => SimpleOperation.Addition(operand1, operand2),
          "*" => SimpleOperation.Multiplication(operand1, operand2),
          "/" => SimpleOperation.Division(operand1, operand2),

          "" => throw new ArgumentException(),
          null => throw new ArgumentNullException(),
          _ => throw new ArgumentOutOfRangeException(),
        };
        answer += calculated.ToString();
      }
      catch (DivideByZeroException e)
      {
        answer = "Division by zero is not allowed.";
      }
      // other exception goes through
      return answer;
    }
  }
}
