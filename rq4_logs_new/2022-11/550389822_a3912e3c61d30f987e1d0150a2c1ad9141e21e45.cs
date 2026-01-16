using System;
using System.Globalization;

namespace Exercism.Property
{
  class WeighingMachine
  {
    double _weight;

    public WeighingMachine(int precision)
    {
      Precision = precision;  // through Properties
    }

    // Auto-implementation
    public int Precision { get; private set; }

    // Field Backed Properties
    public double Weight
    {
      get => _weight;
      set
      {
        if (value < 0) throw new ArgumentOutOfRangeException("cannot be negative");
        _weight = Math.Round(value, Precision);
      }
    }

    // only get accessor
    public string DisplayWeight
    {
      get
      {
        var format = new NumberFormatInfo() { NumberDecimalDigits = Precision };
        // 指定桁数0埋めも、変数でやる場合の方法
        var weightString = (Weight - TareAdjustment).ToString("f", format);
        return $"{weightString} kg";
      }
    }

    // default set value
    public double TareAdjustment { get; set; } = 5.0;
  }
}
