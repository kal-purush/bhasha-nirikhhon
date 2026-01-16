function Calc()
{
    this.add = function (addend, augend)
    {
        return augend + addend;
    };

    this.subtract = function(minuend, subtrahend)
    {
        return minuend - subtrahend;
    };

    this.multiply = function(multiplicand, multiplier)
    {
        return multiplicand * multiplier;
    };

    this.divide = function(dividend, divisor)
    {
        if(divisor === 0)
        {
            return NaN;
        }
        else
        {
            return dividend / divisor;
        }
    }
}