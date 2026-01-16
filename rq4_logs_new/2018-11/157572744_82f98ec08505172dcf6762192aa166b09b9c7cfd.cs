using System;

namespace DSAAlgorythm
{
    public class BigInteger
    {
        // maximum length of the BigInteger in uint (4 bytes)
        // change this to suit the required level of precision.
        public const int MaxLength = 256;

        // primes smaller than 2000 to test the generated prime number

        public static readonly int[] PrimesBelow2000 =
        {
            2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97,
            101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199,
            211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293,
            307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397,
            401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499,
            503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599,
            601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691,
            701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797,
            809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887,
            907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997,
            1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097,
            1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181, 1187, 1193,
            1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291, 1297,
            1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373, 1381, 1399,
            1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499,
            1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583, 1597,
            1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657, 1663, 1667, 1669, 1693, 1697, 1699,
            1709, 1721, 1723, 1733, 1741, 1747, 1753, 1759, 1777, 1783, 1787, 1789,
            1801, 1811, 1823, 1831, 1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889,
            1901, 1907, 1913, 1931, 1933, 1949, 1951, 1973, 1979, 1987, 1993, 1997, 1999
        };


        private uint[] _data; // stores bytes from the Big Integer

        public int DataLength { get; set; } // number of actual chars used


        public BigInteger()
        {
            _data = new uint[MaxLength];
            DataLength = 1;
        }

        public BigInteger(long value)
        {
            _data = new uint[MaxLength];
            long tempVal = value;

            // copy bytes from long to BigInteger without any assumption of
            // the length of the long datatype

            DataLength = 0;
            while (value != 0 && DataLength < MaxLength)
            {
                _data[DataLength] = (uint)(value & 0xFFFFFFFF);
                value >>= 32;
                DataLength++;
            }

            if (tempVal > 0) // overflow check for +ve value
            {
                if (value != 0 || (_data[MaxLength - 1] & 0x80000000) != 0)
                    throw (new ArithmeticException("Positive overflow in constructor."));
            }
            else if (tempVal < 0) // underflow check for -ve value
            {
                if (value != -1 || (_data[DataLength - 1] & 0x80000000) == 0)
                    throw (new ArithmeticException("Negative underflow in constructor."));
            }

            if (DataLength == 0)
                DataLength = 1;
        }


        //***********************************************************************
        // Constructor (Default value provided by ulong)
        //***********************************************************************

        public BigInteger(ulong value)
        {
            _data = new uint[MaxLength];

            // copy bytes from ulong to BigInteger without any assumption of
            // the length of the ulong datatype

            DataLength = 0;
            while (value != 0 && DataLength < MaxLength)
            {
                _data[DataLength] = (uint)(value & 0xFFFFFFFF);
                value >>= 32;
                DataLength++;
            }

            if (value != 0 || (_data[MaxLength - 1] & 0x80000000) != 0)
                throw (new ArithmeticException("Positive overflow in constructor."));

            if (DataLength == 0)
                DataLength = 1;
        }

        public BigInteger(BigInteger bi)
        {
            _data = new uint[MaxLength];

            DataLength = bi.DataLength;

            bi._data.CopyTo(_data, 0);
        }

        // The lowest index of the input byte array (i.e [0]) should contain the
        // most significant byte of the number, and the highest index should
        // contain the least significant byte.
        public BigInteger(byte[] inData)
        {
            DataLength = inData.Length >> 2;

            int leftOver = inData.Length & 0x3;
            if (leftOver != 0) // length not multiples of 4
                DataLength++;


            if (DataLength > MaxLength)
                throw (new ArithmeticException("Byte overflow in constructor."));

            _data = new uint[MaxLength];

            for (int i = inData.Length - 1, j = 0; i >= 3; i -= 4, j++)
            {
                _data[j] = (uint)((inData[i - 3] << 24) + (inData[i - 2] << 16) +
                                  (inData[i - 1] << 8) + inData[i]);
            }

            if (leftOver == 1)
                _data[DataLength - 1] = (uint)inData[0];
            else if (leftOver == 2)
                _data[DataLength - 1] = (uint)((inData[0] << 8) + inData[1]);
            else if (leftOver == 3)
                _data[DataLength - 1] = (uint)((inData[0] << 16) + (inData[1] << 8) + inData[2]);


            while (DataLength > 1 && _data[DataLength - 1] == 0)
                DataLength--;
        }

        //***********************************************************************
        // Constructor (Default value provided by an array of unsigned integers)
        //*********************************************************************

        public BigInteger(uint[] inData)
        {
            DataLength = inData.Length;

            if (DataLength > MaxLength)
                throw (new ArithmeticException("Byte overflow in constructor."));

            _data = new uint[MaxLength];

            for (int i = DataLength - 1, j = 0; i >= 0; i--, j++)
                _data[j] = inData[i];

            while (DataLength > 1 && _data[DataLength - 1] == 0)
                DataLength--;
        }


        //***********************************************************************
        // Overloading of the typecast operator.
        // For BigInteger bi = 10;
        //***********************************************************************

        public static implicit operator BigInteger(long value)
        {
            return (new BigInteger(value));
        }

        public static implicit operator BigInteger(ulong value)
        {
            return (new BigInteger(value));
        }

        public static implicit operator BigInteger(int value)
        {
            return (new BigInteger((long)value));
        }

        public static implicit operator BigInteger(uint value)
        {
            return (new BigInteger((ulong)value));
        }


        //***********************************************************************
        // Overloading of addition operator
        //***********************************************************************

        public static BigInteger operator +(BigInteger bi1, BigInteger bi2)
        {
            BigInteger result = new BigInteger();

            result.DataLength = (bi1.DataLength > bi2.DataLength) ? bi1.DataLength : bi2.DataLength;

            long carry = 0;
            for (int i = 0; i < result.DataLength; i++)
            {
                long sum = (long)bi1._data[i] + (long)bi2._data[i] + carry;
                carry = sum >> 32;
                result._data[i] = (uint)(sum & 0xFFFFFFFF);
            }

            if (carry != 0 && result.DataLength < MaxLength)
            {
                result._data[result.DataLength] = (uint)(carry);
                result.DataLength++;
            }

            while (result.DataLength > 1 && result._data[result.DataLength - 1] == 0)
                result.DataLength--;


            // overflow check
            int lastPos = MaxLength - 1;
            if ((bi1._data[lastPos] & 0x80000000) == (bi2._data[lastPos] & 0x80000000) &&
                (result._data[lastPos] & 0x80000000) != (bi1._data[lastPos] & 0x80000000))
            {
                throw (new ArithmeticException());
            }

            return result;
        }

        public static BigInteger operator -(BigInteger bi1, BigInteger bi2)
        {
            BigInteger result = new BigInteger
            {
                DataLength = (bi1.DataLength > bi2.DataLength) ? bi1.DataLength : bi2.DataLength
            };


            long carryIn = 0;
            for (int i = 0; i < result.DataLength; i++)
            {
                long diff;

                diff = (long)bi1._data[i] - (long)bi2._data[i] - carryIn;
                result._data[i] = (uint)(diff & 0xFFFFFFFF);

                if (diff < 0)
                    carryIn = 1;
                else
                    carryIn = 0;
            }

            // roll over to negative
            if (carryIn != 0)
            {
                for (int i = result.DataLength; i < MaxLength; i++)
                    result._data[i] = 0xFFFFFFFF;
                result.DataLength = MaxLength;
            }

            // fixed in v1.03 to give correct datalength for a - (-b)
            while (result.DataLength > 1 && result._data[result.DataLength - 1] == 0)
                result.DataLength--;

            // overflow check

            int lastPos = MaxLength - 1;
            if ((bi1._data[lastPos] & 0x80000000) != (bi2._data[lastPos] & 0x80000000) &&
                (result._data[lastPos] & 0x80000000) != (bi1._data[lastPos] & 0x80000000))
            {
                throw (new ArithmeticException());
            }

            return result;
        }


        //***********************************************************************
        // Overloading of multiplication operator
        //***********************************************************************

        public static BigInteger operator *(BigInteger bi1, BigInteger bi2)
        {
            int lastPos = MaxLength - 1;
            bool bi1Neg = false, bi2Neg = false;

            // take the absolute value of the inputs
            try
            {
                if ((bi1._data[lastPos] & 0x80000000) != 0) // bi1 negative
                {
                    bi1Neg = true;
                    bi1 = -bi1;
                }

                if ((bi2._data[lastPos] & 0x80000000) != 0) // bi2 negative
                {
                    bi2Neg = true;
                    bi2 = -bi2;
                }
            }
            catch (Exception)
            {
            }

            BigInteger result = new BigInteger();

            // multiply the absolute values
            try
            {
                for (int i = 0; i < bi1.DataLength; i++)
                {
                    if (bi1._data[i] == 0) continue;

                    ulong mcarry = 0;
                    for (int j = 0, k = i; j < bi2.DataLength; j++, k++)
                    {
                        // k = i + j
                        ulong val = ((ulong)bi1._data[i] * (ulong)bi2._data[j]) +
                                    (ulong)result._data[k] + mcarry;

                        result._data[k] = (uint)(val & 0xFFFFFFFF);
                        mcarry = (val >> 32);
                    }

                    if (mcarry != 0)
                        result._data[i + bi2.DataLength] = (uint)mcarry;
                }
            }
            catch (Exception)
            {
                throw (new ArithmeticException("Multiplication overflow."));
            }


            result.DataLength = bi1.DataLength + bi2.DataLength;
            if (result.DataLength > MaxLength)
                result.DataLength = MaxLength;

            while (result.DataLength > 1 && result._data[result.DataLength - 1] == 0)
                result.DataLength--;

            // overflow check (result is -ve)
            if ((result._data[lastPos] & 0x80000000) != 0)
            {
                if (bi1Neg != bi2Neg && result._data[lastPos] == 0x80000000) // different sign
                {
                    // handle the special case where multiplication produces
                    // a max negative number in 2's complement.

                    if (result.DataLength == 1)
                        return result;
                    else
                    {
                        bool isMaxNeg = true;
                        for (int i = 0; i < result.DataLength - 1 && isMaxNeg; i++)
                        {
                            if (result._data[i] != 0)
                                isMaxNeg = false;
                        }

                        if (isMaxNeg)
                            return result;
                    }
                }

                throw (new ArithmeticException("Multiplication overflow."));
            }

            // if input has different signs, then result is -ve
            if (bi1Neg != bi2Neg)
                return -result;

            return result;
        }


        //***********************************************************************
        // Overloading of unary << operators
        //***********************************************************************

        public static BigInteger operator <<(BigInteger bi1, int shiftVal)
        {
            BigInteger result = new BigInteger(bi1);
            result.DataLength = shiftLeft(result._data, shiftVal);

            return result;
        }


        // least significant bits at lower part of buffer

        private static int shiftLeft(uint[] buffer, int shiftVal)
        {
            int shiftAmount = 32;
            int bufLen = buffer.Length;

            while (bufLen > 1 && buffer[bufLen - 1] == 0)
                bufLen--;

            for (int count = shiftVal; count > 0;)
            {
                if (count < shiftAmount)
                    shiftAmount = count;

                //Console.WriteLine("shiftAmount = {0}", shiftAmount);

                ulong carry = 0;
                for (int i = 0; i < bufLen; i++)
                {
                    ulong val = ((ulong)buffer[i]) << shiftAmount;
                    val |= carry;

                    buffer[i] = (uint)(val & 0xFFFFFFFF);
                    carry = val >> 32;
                }

                if (carry != 0)
                {
                    if (bufLen + 1 <= buffer.Length)
                    {
                        buffer[bufLen] = (uint)carry;
                        bufLen++;
                    }
                }

                count -= shiftAmount;
            }

            return bufLen;
        }


        //***********************************************************************
        // Overloading of unary >> operators
        //***********************************************************************

        public static BigInteger operator >>(BigInteger bi1, int shiftVal)
        {
            BigInteger result = new BigInteger(bi1);
            result.DataLength = shiftRight(result._data, shiftVal);


            if ((bi1._data[MaxLength - 1] & 0x80000000) != 0) // negative
            {
                for (int i = MaxLength - 1; i >= result.DataLength; i--)
                    result._data[i] = 0xFFFFFFFF;

                uint mask = 0x80000000;
                for (int i = 0; i < 32; i++)
                {
                    if ((result._data[result.DataLength - 1] & mask) != 0)
                        break;

                    result._data[result.DataLength - 1] |= mask;
                    mask >>= 1;
                }

                result.DataLength = MaxLength;
            }

            return result;
        }


        private static int shiftRight(uint[] buffer, int shiftVal)
        {
            int shiftAmount = 32;
            int invShift = 0;
            int bufLen = buffer.Length;

            while (bufLen > 1 && buffer[bufLen - 1] == 0)
                bufLen--;

            //Console.WriteLine("bufLen = " + bufLen + " buffer.Length = " + buffer.Length);

            for (int count = shiftVal; count > 0;)
            {
                if (count < shiftAmount)
                {
                    shiftAmount = count;
                    invShift = 32 - shiftAmount;
                }

                //Console.WriteLine("shiftAmount = {0}", shiftAmount);

                ulong carry = 0;
                for (int i = bufLen - 1; i >= 0; i--)
                {
                    ulong val = ((ulong)buffer[i]) >> shiftAmount;
                    val |= carry;

                    carry = ((ulong)buffer[i]) << invShift;
                    buffer[i] = (uint)(val);
                }

                count -= shiftAmount;
            }

            while (bufLen > 1 && buffer[bufLen - 1] == 0)
                bufLen--;

            return bufLen;
        }

        //***********************************************************************
        // Overloading of the NEGATE operator (2's complement)
        //***********************************************************************

        public static BigInteger operator -(BigInteger bi1)
        {
            // handle neg of zero separately since it'll cause an overflow
            // if we proceed.

            if (bi1.DataLength == 1 && bi1._data[0] == 0)
                return (new BigInteger());

            BigInteger result = new BigInteger(bi1);

            // 1's complement
            for (int i = 0; i < MaxLength; i++)
                result._data[i] = (uint)(~(bi1._data[i]));

            // add one to result of 1's complement
            long val, carry = 1;
            int index = 0;

            while (carry != 0 && index < MaxLength)
            {
                val = (long)(result._data[index]);
                val++;

                result._data[index] = (uint)(val & 0xFFFFFFFF);
                carry = val >> 32;

                index++;
            }

            if ((bi1._data[MaxLength - 1] & 0x80000000) == (result._data[MaxLength - 1] & 0x80000000))
                throw (new ArithmeticException("Overflow in negation.\n"));

            result.DataLength = MaxLength;

            while (result.DataLength > 1 && result._data[result.DataLength - 1] == 0)
                result.DataLength--;
            return result;
        }


        //***********************************************************************
        // Overloading of equality operator
        //***********************************************************************

        public static bool operator ==(BigInteger bi1, BigInteger bi2)
        {
            return bi1.Equals(bi2);
        }


        public static bool operator !=(BigInteger bi1, BigInteger bi2)
        {
            return !(bi1.Equals(bi2));
        }


        public override bool Equals(object o)
        {
            BigInteger bi = (BigInteger)o;

            if (this.DataLength != bi.DataLength)
                return false;

            for (int i = 0; i < this.DataLength; i++)
            {
                if (this._data[i] != bi._data[i])
                    return false;
            }

            return true;
        }


        public override int GetHashCode()
        {
            return this.ToString().GetHashCode();
        }


        //***********************************************************************
        // Overloading of inequality operator
        //***********************************************************************

        public static bool operator >(BigInteger bi1, BigInteger bi2)
        {
            int pos = MaxLength - 1;

            // bi1 is negative, bi2 is positive
            if ((bi1._data[pos] & 0x80000000) != 0 && (bi2._data[pos] & 0x80000000) == 0)
                return false;

            // bi1 is positive, bi2 is negative
            else if ((bi1._data[pos] & 0x80000000) == 0 && (bi2._data[pos] & 0x80000000) != 0)
                return true;

            // same sign
            int len = (bi1.DataLength > bi2.DataLength) ? bi1.DataLength : bi2.DataLength;
            for (pos = len - 1; pos >= 0 && bi1._data[pos] == bi2._data[pos]; pos--) ;

            if (pos >= 0)
            {
                if (bi1._data[pos] > bi2._data[pos])
                    return true;
                return false;
            }

            return false;
        }


        public static bool operator <(BigInteger bi1, BigInteger bi2)
        {
            int pos = MaxLength - 1;

            // bi1 is negative, bi2 is positive
            if ((bi1._data[pos] & 0x80000000) != 0 && (bi2._data[pos] & 0x80000000) == 0)
                return true;

            // bi1 is positive, bi2 is negative
            else if ((bi1._data[pos] & 0x80000000) == 0 && (bi2._data[pos] & 0x80000000) != 0)
                return false;

            // same sign
            int len = (bi1.DataLength > bi2.DataLength) ? bi1.DataLength : bi2.DataLength;
            for (pos = len - 1; pos >= 0 && bi1._data[pos] == bi2._data[pos]; pos--) ;

            if (pos >= 0)
            {
                if (bi1._data[pos] < bi2._data[pos])
                    return true;
                return false;
            }

            return false;
        }


        public static bool operator >=(BigInteger bi1, BigInteger bi2)
        {
            return (bi1 == bi2 || bi1 > bi2);
        }


        public static bool operator <=(BigInteger bi1, BigInteger bi2)
        {
            return (bi1 == bi2 || bi1 < bi2);
        }


        //***********************************************************************
        // Private function that supports the division of two numbers with
        // a divisor that has more than 1 digit.
        //
        // Algorithm taken from [1]
        //***********************************************************************

        private static void multiByteDivide(BigInteger bi1, BigInteger bi2,
            BigInteger outQuotient, BigInteger outRemainder)
        {
            uint[] result = new uint[MaxLength];

            int remainderLen = bi1.DataLength + 1;
            uint[] remainder = new uint[remainderLen];

            uint mask = 0x80000000;
            uint val = bi2._data[bi2.DataLength - 1];
            int shift = 0, resultPos = 0;

            while (mask != 0 && (val & mask) == 0)
            {
                shift++;
                mask >>= 1;
            }

            //Console.WriteLine("shift = {0}", shift);
            //Console.WriteLine("Before bi1 Len = {0}, bi2 Len = {1}", bi1.DataLength, bi2.DataLength);

            for (int i = 0; i < bi1.DataLength; i++)
                remainder[i] = bi1._data[i];
            shiftLeft(remainder, shift);
            bi2 = bi2 << shift;

            /*
            Console.WriteLine("bi1 Len = {0}, bi2 Len = {1}", bi1.DataLength, bi2.DataLength);
            Console.WriteLine("dividend = " + bi1 + "\ndivisor = " + bi2);
            for(int q = remainderLen - 1; q >= 0; q--)
                    Console.Write("{0:x2}", remainder[q]);
            Console.WriteLine();
            */

            int j = remainderLen - bi2.DataLength;
            int pos = remainderLen - 1;

            ulong firstDivisorByte = bi2._data[bi2.DataLength - 1];
            ulong secondDivisorByte = bi2._data[bi2.DataLength - 2];

            int divisorLen = bi2.DataLength + 1;
            uint[] dividendPart = new uint[divisorLen];

            while (j > 0)
            {
                ulong dividend = ((ulong)remainder[pos] << 32) + (ulong)remainder[pos - 1];
                //Console.WriteLine("dividend = {0}", dividend);

                ulong q_hat = dividend / firstDivisorByte;
                ulong r_hat = dividend % firstDivisorByte;

                //Console.WriteLine("q_hat = {0:X}, r_hat = {1:X}", q_hat, r_hat);

                bool done = false;
                while (!done)
                {
                    done = true;

                    if (q_hat == 0x100000000 ||
                        (q_hat * secondDivisorByte) > ((r_hat << 32) + remainder[pos - 2]))
                    {
                        q_hat--;
                        r_hat += firstDivisorByte;

                        if (r_hat < 0x100000000)
                            done = false;
                    }
                }

                for (int h = 0; h < divisorLen; h++)
                    dividendPart[h] = remainder[pos - h];

                BigInteger kk = new BigInteger(dividendPart);
                BigInteger ss = bi2 * (long)q_hat;

                //Console.WriteLine("ss before = " + ss);
                while (ss > kk)
                {
                    q_hat--;
                    ss -= bi2;
                    //Console.WriteLine(ss);
                }

                BigInteger yy = kk - ss;


                for (int h = 0; h < divisorLen; h++)
                    remainder[pos - h] = yy._data[bi2.DataLength - h];


                result[resultPos++] = (uint)q_hat;

                pos--;
                j--;
            }

            outQuotient.DataLength = resultPos;
            int y = 0;
            for (int x = outQuotient.DataLength - 1; x >= 0; x--, y++)
                outQuotient._data[y] = result[x];
            for (; y < MaxLength; y++)
                outQuotient._data[y] = 0;

            while (outQuotient.DataLength > 1 && outQuotient._data[outQuotient.DataLength - 1] == 0)
                outQuotient.DataLength--;

            if (outQuotient.DataLength == 0)
                outQuotient.DataLength = 1;

            outRemainder.DataLength = shiftRight(remainder, shift);

            for (y = 0; y < outRemainder.DataLength; y++)
                outRemainder._data[y] = remainder[y];
            for (; y < MaxLength; y++)
                outRemainder._data[y] = 0;
        }


        //***********************************************************************
        // Private function that supports the division of two numbers with
        // a divisor that has only 1 digit.
        //***********************************************************************

        private static void singleByteDivide(BigInteger bi1, BigInteger bi2,
            BigInteger outQuotient, BigInteger outRemainder)
        {
            uint[] result = new uint[MaxLength];
            int resultPos = 0;

            // copy dividend to reminder
            for (int i = 0; i < MaxLength; i++)
                outRemainder._data[i] = bi1._data[i];
            outRemainder.DataLength = bi1.DataLength;

            while (outRemainder.DataLength > 1 && outRemainder._data[outRemainder.DataLength - 1] == 0)
                outRemainder.DataLength--;

            ulong divisor = (ulong)bi2._data[0];
            int pos = outRemainder.DataLength - 1;
            ulong dividend = (ulong)outRemainder._data[pos];

            if (dividend >= divisor)
            {
                ulong quotient = dividend / divisor;
                result[resultPos++] = (uint)quotient;

                outRemainder._data[pos] = (uint)(dividend % divisor);
            }

            pos--;

            while (pos >= 0)
            {

                dividend = ((ulong)outRemainder._data[pos + 1] << 32) + (ulong)outRemainder._data[pos];
                ulong quotient = dividend / divisor;
                result[resultPos++] = (uint)quotient;

                outRemainder._data[pos + 1] = 0;
                outRemainder._data[pos--] = (uint)(dividend % divisor);
            }

            outQuotient.DataLength = resultPos;
            int j = 0;
            for (int i = outQuotient.DataLength - 1; i >= 0; i--, j++)
                outQuotient._data[j] = result[i];
            for (; j < MaxLength; j++)
                outQuotient._data[j] = 0;

            while (outQuotient.DataLength > 1 && outQuotient._data[outQuotient.DataLength - 1] == 0)
                outQuotient.DataLength--;

            if (outQuotient.DataLength == 0)
                outQuotient.DataLength = 1;

            while (outRemainder.DataLength > 1 && outRemainder._data[outRemainder.DataLength - 1] == 0)
                outRemainder.DataLength--;
        }


        //***********************************************************************
        // Overloading of division operator
        //***********************************************************************

        public static BigInteger operator /(BigInteger bi1, BigInteger bi2)
        {
            BigInteger quotient = new BigInteger();
            BigInteger remainder = new BigInteger();

            int lastPos = MaxLength - 1;
            bool divisorNeg = false, dividendNeg = false;

            if ((bi1._data[lastPos] & 0x80000000) != 0) // bi1 negative
            {
                bi1 = -bi1;
                dividendNeg = true;
            }

            if ((bi2._data[lastPos] & 0x80000000) != 0) // bi2 negative
            {
                bi2 = -bi2;
                divisorNeg = true;
            }

            if (bi1 < bi2)
            {
                return quotient;
            }

            else
            {
                if (bi2.DataLength == 1)
                    singleByteDivide(bi1, bi2, quotient, remainder);
                else
                    multiByteDivide(bi1, bi2, quotient, remainder);

                if (dividendNeg != divisorNeg)
                    return -quotient;

                return quotient;
            }
        }


        //***********************************************************************
        // Overloading of modulus operator
        //***********************************************************************

        public static BigInteger operator %(BigInteger bi1, BigInteger bi2)
        {
            BigInteger quotient = new BigInteger();
            BigInteger remainder = new BigInteger(bi1);

            int lastPos = MaxLength - 1;
            bool dividendNeg = false;

            if ((bi1._data[lastPos] & 0x80000000) != 0) // bi1 negative
            {
                bi1 = -bi1;
                dividendNeg = true;
            }

            if ((bi2._data[lastPos] & 0x80000000) != 0) // bi2 negative
                bi2 = -bi2;

            if (bi1 < bi2)
            {
                return remainder;
            }

            else
            {
                if (bi2.DataLength == 1)
                    singleByteDivide(bi1, bi2, quotient, remainder);
                else
                    multiByteDivide(bi1, bi2, quotient, remainder);

                if (dividendNeg)
                    return -remainder;

                return remainder;
            }
        }

        //***********************************************************************
        // Returns a string representing the BigInteger in base 10.
        //***********************************************************************

        public override string ToString()
        {
            return ToString(10);
        }


        //***********************************************************************
        // Returns a string representing the BigInteger in sign-and-magnitude
        // format in the specified radix.
        //
        // Example
        // -------
        // If the value of BigInteger is -255 in base 10, then
        // ToString(16) returns "-FF"
        //
        //***********************************************************************

        public string ToString(int radix)
        {
            if (radix < 2 || radix > 36)
                throw (new ArgumentException("Radix must be >= 2 and <= 36"));

            string charSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            string result = "";

            BigInteger a = this;

            bool negative = false;
            if ((a._data[MaxLength - 1] & 0x80000000) != 0)
            {
                negative = true;
                try
                {
                    a = -a;
                }
                catch (Exception)
                {
                }
            }

            BigInteger quotient = new BigInteger();
            BigInteger remainder = new BigInteger();
            BigInteger biRadix = new BigInteger(radix);

            if (a.DataLength == 1 && a._data[0] == 0)
                result = "0";
            else
            {
                while (a.DataLength > 1 || (a.DataLength == 1 && a._data[0] != 0))
                {
                    singleByteDivide(a, biRadix, quotient, remainder);

                    if (remainder._data[0] < 10)
                        result = remainder._data[0] + result;
                    else
                        result = charSet[(int)remainder._data[0] - 10] + result;

                    a = quotient;
                }

                if (negative)
                    result = "-" + result;
            }

            return result;
        }


        //***********************************************************************
        // Modulo Exponentiation
        //***********************************************************************

        public BigInteger ModPow(BigInteger exp, BigInteger n)
        {
            if ((exp._data[MaxLength - 1] & 0x80000000) != 0)
                throw (new ArithmeticException("Positive exponents only."));

            BigInteger resultNum = 1;
            BigInteger tempNum;
            bool thisNegative = false;

            if ((this._data[MaxLength - 1] & 0x80000000) != 0) // negative this
            {
                tempNum = -this % n;
                thisNegative = true;
            }
            else
                tempNum = this % n; // ensures (tempNum * tempNum) < b^(2k)

            if ((n._data[MaxLength - 1] & 0x80000000) != 0) // negative n
                n = -n;

            // calculate constant = b^(2k) / m
            BigInteger constant = new BigInteger();

            int i = n.DataLength << 1;
            constant._data[i] = 0x00000001;
            constant.DataLength = i + 1;

            constant = constant / n;
            int totalBits = exp.BitCount();
            int count = 0;

            // perform squaring and multiply exponentiation
            for (int pos = 0; pos < exp.DataLength; pos++)
            {
                uint mask = 0x01;
                //Console.WriteLine("pos = " + pos);

                for (int index = 0; index < 32; index++)
                {
                    if ((exp._data[pos] & mask) != 0)
                        resultNum = BarrettReduction(resultNum * tempNum, n, constant);

                    mask <<= 1;

                    tempNum = BarrettReduction(tempNum * tempNum, n, constant);


                    if (tempNum.DataLength == 1 && tempNum._data[0] == 1)
                    {
                        if (thisNegative && (exp._data[0] & 0x1) != 0) //odd exp
                            return -resultNum;
                        return resultNum;
                    }

                    count++;
                    if (count == totalBits)
                        break;
                }
            }

            if (thisNegative && (exp._data[0] & 0x1) != 0) //odd exp
                return -resultNum;

            return resultNum;
        }


        //***********************************************************************
        // Fast calculation of modular reduction using Barrett's reduction.
        // Requires x < b^(2k), where b is the base.  In this case, base is
        // 2^32 (uint).
        //
        // Reference [4]
        //***********************************************************************

        private BigInteger BarrettReduction(BigInteger x, BigInteger n, BigInteger constant)
        {
            int k = n.DataLength,
                kPlusOne = k + 1,
                kMinusOne = k - 1;

            BigInteger q1 = new BigInteger();

            // q1 = x / b^(k-1)
            for (int i = kMinusOne, j = 0; i < x.DataLength; i++, j++)
                q1._data[j] = x._data[i];
            q1.DataLength = x.DataLength - kMinusOne;
            if (q1.DataLength <= 0)
                q1.DataLength = 1;


            BigInteger q2 = q1 * constant;
            BigInteger q3 = new BigInteger();

            // q3 = q2 / b^(k+1)
            for (int i = kPlusOne, j = 0; i < q2.DataLength; i++, j++)
                q3._data[j] = q2._data[i];
            q3.DataLength = q2.DataLength - kPlusOne;
            if (q3.DataLength <= 0)
                q3.DataLength = 1;


            // r1 = x mod b^(k+1)
            // i.e. keep the lowest (k+1) words
            BigInteger r1 = new BigInteger();
            int lengthToCopy = (x.DataLength > kPlusOne) ? kPlusOne : x.DataLength;
            for (int i = 0; i < lengthToCopy; i++)
                r1._data[i] = x._data[i];
            r1.DataLength = lengthToCopy;


            // r2 = (q3 * n) mod b^(k+1)
            // partial multiplication of q3 and n

            BigInteger r2 = new BigInteger();
            for (int i = 0; i < q3.DataLength; i++)
            {
                if (q3._data[i] == 0) continue;

                ulong mcarry = 0;
                int t = i;
                for (int j = 0; j < n.DataLength && t < kPlusOne; j++, t++)
                {
                    // t = i + j
                    ulong val = ((ulong)q3._data[i] * (ulong)n._data[j]) +
                                (ulong)r2._data[t] + mcarry;

                    r2._data[t] = (uint)(val & 0xFFFFFFFF);
                    mcarry = (val >> 32);
                }

                if (t < kPlusOne)
                    r2._data[t] = (uint)mcarry;
            }

            r2.DataLength = kPlusOne;
            while (r2.DataLength > 1 && r2._data[r2.DataLength - 1] == 0)
                r2.DataLength--;

            r1 -= r2;
            if ((r1._data[MaxLength - 1] & 0x80000000) != 0) // negative
            {
                BigInteger val = new BigInteger();
                val._data[kPlusOne] = 0x00000001;
                val.DataLength = kPlusOne + 1;
                r1 += val;
            }

            while (r1 >= n)
                r1 -= n;

            return r1;
        }


        //***********************************************************************
        // Returns Gcd(this, bi)
        //***********************************************************************

        public BigInteger Gcd(BigInteger bi)
        {
            BigInteger x;
            BigInteger y;

            if ((_data[MaxLength - 1] & 0x80000000) != 0) // negative
                x = -this;
            else
                x = this;

            if ((bi._data[MaxLength - 1] & 0x80000000) != 0) // negative
                y = -bi;
            else
                y = bi;

            BigInteger g = y;

            while (x.DataLength > 1 || (x.DataLength == 1 && x._data[0] != 0))
            {
                g = x;
                x = y % x;
                y = g;
            }

            return g;
        }


        //***********************************************************************
        // Populates "this" with the specified amount of random bits
        //***********************************************************************

        public void GenerateRandomBits(int bits, Random rand)
        {
            int dwords = bits >> 5;
            int remBits = bits & 0x1F;

            if (remBits != 0)
                dwords++;

            if (dwords > MaxLength)
                throw (new ArithmeticException("Number of required bits > MaxLength."));

            for (int i = 0; i < dwords; i++)
                _data[i] = (uint)(rand.NextDouble() * 0x100000000);

            for (int i = dwords; i < MaxLength; i++)
                _data[i] = 0;

            if (remBits != 0)
            {
                uint mask = (uint)(0x01 << (remBits - 1));
                _data[dwords - 1] |= mask;

                mask = (uint)(0xFFFFFFFF >> (32 - remBits));
                _data[dwords - 1] &= mask;
            }
            else
                _data[dwords - 1] |= 0x80000000;

            DataLength = dwords;

            if (DataLength == 0)
                DataLength = 1;
        }

        public void GenerateRandomBitsFromZero(int bits, Random rand)
        {
            int dwords = bits >> 5;
            int remBits = bits & 0x1F;

            if (remBits != 0)
                dwords++;

            if (dwords > MaxLength)
                throw (new ArithmeticException("Number of required bits > MaxLength."));

            for (int i = 0; i < dwords; i++)
                _data[i] = (uint)(rand.NextDouble() * 0x100000000);

            for (int i = dwords; i < MaxLength; i++)
                _data[i] = 0;

            if (remBits != 0)
            {
                uint mask = (uint)(0xFFFFFFFF >> (32 - remBits));
                _data[dwords - 1] &= mask;
            }

            DataLength = dwords;

            if (DataLength == 0)
                DataLength = 1;
        }


        //***********************************************************************
        // Returns the position of the most significant bit in the BigInteger.
        //
        // Eg.  The result is 0, if the value of BigInteger is 0...0000 0000
        //      The result is 1, if the value of BigInteger is 0...0000 0001
        //      The result is 2, if the value of BigInteger is 0...0000 0010
        //      The result is 2, if the value of BigInteger is 0...0000 0011
        //
        //***********************************************************************

        public int BitCount()
        {
            while (DataLength > 1 && _data[DataLength - 1] == 0)
                DataLength--;

            uint value = _data[DataLength - 1];
            uint mask = 0x80000000;
            int bits = 32;

            while (bits > 0 && (value & mask) == 0)
            {
                bits--;
                mask >>= 1;
            }

            bits += ((DataLength - 1) << 5);

            return bits;
        }


        //***********************************************************************
        // Probabilistic prime test based on Rabin-Miller's
        //
        // for any p > 0 with p - 1 = 2^s * t
        //
        // p is probably prime (strong pseudoprime) if for any a < p,
        // 1) a^t mod p = 1 or
        // 2) a^((2^j)*t) mod p = p-1 for some 0 <= j <= s-1
        //
        // Otherwise, p is composite.
        //
        // Returns
        // -------
        // True if "this" is a strong pseudoprime to randomly chosen
        // bases.  The number of chosen bases is given by the "confidence"
        // parameter.
        //
        // False if "this" is definitely NOT prime.
        //
        //***********************************************************************

        public bool RabinMillerTest(int confidence)
        {
            BigInteger thisVal;
            if ((this._data[MaxLength - 1] & 0x80000000) != 0) // negative
                thisVal = -this;
            else
                thisVal = this;

            if (thisVal.DataLength == 1)
            {
                // test small numbers
                if (thisVal._data[0] == 0 || thisVal._data[0] == 1)
                    return false;
                else if (thisVal._data[0] == 2 || thisVal._data[0] == 3)
                    return true;
            }

            if ((thisVal._data[0] & 0x1) == 0) // even numbers
                return false;


            // calculate values of s and t
            BigInteger p_sub1 = thisVal - (new BigInteger(1));
            int s = 0;

            for (int index = 0; index < p_sub1.DataLength; index++)
            {
                uint mask = 0x01;

                for (int i = 0; i < 32; i++)
                {
                    if ((p_sub1._data[index] & mask) != 0)
                    {
                        index = p_sub1.DataLength; // to break the outer loop
                        break;
                    }

                    mask <<= 1;
                    s++;
                }
            }

            BigInteger t = p_sub1 >> s;

            int bits = thisVal.BitCount();
            BigInteger a = new BigInteger();
            Random rand = new Random();

            for (int round = 0; round < confidence; round++)
            {
                bool done = false;

                while (!done) // generate a < n
                {
                    int testBits = 0;

                    // make sure "a" has at least 2 bits
                    while (testBits < 2)
                        testBits = (int)(rand.NextDouble() * bits);

                    a.GenerateRandomBits(testBits, rand);

                    int byteLen = a.DataLength;

                    // make sure "a" is not 0
                    if (byteLen > 1 || (byteLen == 1 && a._data[0] != 1))
                        done = true;
                }

                // check whether a factor exists (fix for version 1.03)
                BigInteger gcdTest = a.Gcd(thisVal);
                if (gcdTest.DataLength == 1 && gcdTest._data[0] != 1)
                    return false;

                BigInteger b = a.ModPow(t, thisVal);

                bool result = false;

                if (b.DataLength == 1 && b._data[0] == 1) // a^t mod p = 1
                    result = true;

                for (int j = 0; result == false && j < s; j++)
                {
                    if (b == p_sub1) // a^((2^j)*t) mod p = p-1 for some 0 <= j <= s-1
                    {
                        result = true;
                        break;
                    }

                    b = (b * b) % thisVal;
                }

                if (result == false)
                    return false;
            }

            return true;
        }


        //***********************************************************************
        // Determines whether a number is probably prime, using the Rabin-Miller's
        // test.  Before applying the test, the number is tested for divisibility
        // by primes < 2000
        //
        // Returns true if number is probably prime.
        //***********************************************************************

        public bool IsProbablePrime(int confidence)
        {
            BigInteger thisVal;
            if ((this._data[MaxLength - 1] & 0x80000000) != 0) // negative
                thisVal = -this;
            else
                thisVal = this;


            // test for divisibility by primes < 2000
            for (int p = 0; p < PrimesBelow2000.Length; p++)
            {
                BigInteger divisor = PrimesBelow2000[p];

                if (divisor >= thisVal)
                    break;

                BigInteger resultNum = thisVal % divisor;
                if (resultNum.IntValue() == 0)
                {
                    /*
    Console.WriteLine("Not prime!  Divisible by {0}\n",
                                      primesBelow2000[p]);
                    */
                    return false;
                }
            }

            if (thisVal.RabinMillerTest(confidence))
                return true;
            else
            {
                //Console.WriteLine("Not prime!  Failed primality test\n");
                return false;
            }
        }


        //***********************************************************************
        // Returns the lowest 4 bytes of the BigInteger as an int.
        //***********************************************************************

        public int IntValue()
        {
            return (int)_data[0];
        }

        //***********************************************************************
        // Generates a positive BigInteger that is probably prime.
        //***********************************************************************

        public static BigInteger GeneratePseudoPrime(int bits, int confidence, Random rand)
        {
            BigInteger result = new BigInteger();
            bool done = false;

            while (!done)
            {
                result.GenerateRandomBits(bits, rand);
                result._data[0] |= 0x01; // make it odd

                // prime test
                done = result.IsProbablePrime(confidence);
            }

            return result;
        }

        //***********************************************************************
        // Returns the value of the BigInteger as a byte array.  The lowest
        // index contains the MSB.
        //***********************************************************************

        public byte[] GetBytes()
        {
            int numBits = BitCount();

            int numBytes = numBits >> 3;
            if ((numBits & 0x7) != 0)
                numBytes++;

            byte[] result = new byte[numBytes];

            //Console.WriteLine(result.Length);

            int pos = 0;
            uint tempVal, val = _data[DataLength - 1];

            if ((tempVal = (val >> 24 & 0xFF)) != 0)
                result[pos++] = (byte)tempVal;
            if ((tempVal = (val >> 16 & 0xFF)) != 0)
                result[pos++] = (byte)tempVal;
            if ((tempVal = (val >> 8 & 0xFF)) != 0)
                result[pos++] = (byte)tempVal;
            if ((tempVal = (val & 0xFF)) != 0)
                result[pos++] = (byte)tempVal;

            for (int i = DataLength - 2; i >= 0; i--, pos += 4)
            {
                val = _data[i];
                result[pos + 3] = (byte)(val & 0xFF);
                val >>= 8;
                result[pos + 2] = (byte)(val & 0xFF);
                val >>= 8;
                result[pos + 1] = (byte)(val & 0xFF);
                val >>= 8;
                result[pos] = (byte)(val & 0xFF);
            }

            return result;
        }

        public byte[] GetAllBytes()
        {
            int numBits = BitCount();

            int numBytes = numBits >> 3;
            if ((numBits & 0x7) != 0)
                numBytes++;

            byte[] result = new byte[numBytes];

            int pos = 0;
            uint tempVal, val = _data[DataLength - 1];

            bool isHighestByteFound = false;

            var shiftedVal = val >> 24;
            var andVal = shiftedVal & 0xFF;
            tempVal = andVal;

            if (tempVal != 0)
            {

                result[pos++] = (byte)tempVal;
                isHighestByteFound = true;
            }

            shiftedVal = val >> 16;
            andVal = shiftedVal & 0xFF;
            tempVal = andVal;

            if (isHighestByteFound || tempVal != 0)
            {
                result[pos++] = (byte)tempVal;
                isHighestByteFound = true;
            }

            shiftedVal = val >> 8;
            andVal = shiftedVal & 0xFF;
            tempVal = andVal;

            if (isHighestByteFound || tempVal != 0)
            {
                result[pos++] = (byte)tempVal;
                isHighestByteFound = true;
            }

            andVal = val & 0xFF;
            tempVal = andVal;

            if (isHighestByteFound || tempVal != 0)
            {
                result[pos++] = (byte)tempVal;
            }

            for (int i = DataLength - 2; i >= 0; i--, pos += 4)
            {
                val = _data[i];
                result[pos + 3] = (byte)(val & 0xFF);
                val >>= 8;
                result[pos + 2] = (byte)(val & 0xFF);
                val >>= 8;
                result[pos + 1] = (byte)(val & 0xFF);
                val >>= 8;
                result[pos] = (byte)(val & 0xFF);
            }

            return result;
        }
    }
}