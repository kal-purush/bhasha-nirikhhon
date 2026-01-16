using System;
using System.Collections.Generic;
using System.Linq;

namespace DSAAlgorythm
{
    public class BigInt
    {
        public static uint MAX_DIGIT = UInt32.MaxValue;

        #region PROPERTIES AND INDEXERS 
        private DoubleWordsCollection _doubleWords;
        public int Length => _doubleWords.DigitsCount;
        public bool IsZero => _doubleWords.DigitsCount == 1 && _doubleWords[0] == 0;
        public uint this[int i] => _doubleWords[i];
        public List<uint> Digits => _doubleWords.GetDigits;

        #endregion

        //uint[] LSB (least significant bit)
        public BigInt(uint[] doubleWords)
        {
            if (doubleWords == null) throw new ArgumentNullException();

            _doubleWords = new DoubleWordsCollection(doubleWords);
        }

        public BigInt(DoubleWordsCollection doubleWords)
        {
            _doubleWords = new DoubleWordsCollection(doubleWords);
        }

        public static BigInt PowMod(BigInt powBase, BigInt exponent, BigInt modulus)
        {
            BigInt res = new BigInt(new uint[] { 1 });

            powBase = powBase % modulus;
            int counter = 0;
            int limit = exponent._doubleWords.GetLeftmostActiveBitIndex();
            while (counter <= limit)
            {
                if (exponent._doubleWords.GetBit(counter))
                    res = (res * powBase) % modulus;

                powBase = (powBase * powBase);
                powBase = powBase % modulus;
                counter++;
            }
            return res;
        }

        public static BigInt operator %(BigInt self, BigInt mod)
        {
            Divide(self, mod, out BigInt remainder);
            return remainder;
        }

        public static BigInt Divide(BigInt dividend, BigInt divisor, out BigInt remainder)
        {
            if (dividend < divisor)
            {
                remainder = new BigInt(dividend._doubleWords);
                return new BigInt(new uint[] { 0 });
            }

            BigInt tmpBig;
            int i = dividend.Length - divisor.Length;
            List<uint> divisorDigits = divisor._doubleWords.GetDigits;
            List<uint> tmp = dividend._doubleWords.GetDigits.Skip(i).ToList();
            i--;

            if (!tmp.IsEqualOrGreaterThan(divisorDigits))
            {
                tmp.Insert(0, dividend[i]);
                i--;
            }

            tmpBig = new BigInt(tmp.ToArray());


            List<uint> result = new List<uint>();
            for (; i >= 0 || tmpBig >= divisor;)
            {
                uint counter = 0;
                while (tmpBig >= divisor)
                {
                    tmpBig -= divisor;
                    counter++;
                }
                result.Insert(0, counter);


                tmp = tmpBig._doubleWords.GetDigits;
                if (i >= 0)
                {
                    tmp.Insert(0, dividend[i]);
                    i--;
                }

                while (i >= 0 && !tmp.IsEqualOrGreaterThan(divisorDigits))
                {
                    tmp.Insert(0, dividend[i]);
                    result.Insert(0, 0);
                    i--;
                }

                tmpBig = new BigInt(tmp.ToArray());
                if (!tmp.IsEqualOrGreaterThan(divisorDigits)) break;
            }

            remainder = tmpBig;
            return new BigInt(result.ToArray());


        }

        public static BigInt operator *(BigInt self, BigInt other)
        {
            BigInt bigger, smaller;
            if (self > other)
            {
                bigger = self;
                smaller = other;
            }
            else
            {
                bigger = other;
                smaller = self;
            }

            int maxSize = smaller.Length + bigger.Length;
            List<uint> result = new List<uint>(maxSize);
            result.AddRange(new uint[maxSize - 1]);

            ulong carry = 0;
            for (int i = 0; i < smaller.Length; i++)
            {
                for (int j = 0; j < bigger.Length; j++)
                {
                    carry += (ulong)smaller[i] * (ulong)bigger[j] + (ulong)result[i + j];
                    result[i + j] = (uint)carry;
                    carry >>= DoubleWordsCollection.BitsInDWord;
                }
            }

            if (carry != 0) result.Add((uint)carry);

            return new BigInt(result.ToArray());
        }

        public static BigInt operator +(BigInt self, BigInt other)
        {
            BigInt bigger, smaller;
            if (self > other)
            {
                bigger = self;
                smaller = other;
            }
            else
            {
                bigger = other;
                smaller = self;
            }

            //set list's initial capacity to contain maximum result; to avoid resizing of backed-array
            int maxSize = bigger.Length + 1;
            List<uint> result = new List<uint>(maxSize);
            ulong carry = 0;
            int i = 0;

            while (i < smaller.Length)
            {
                carry += (ulong)bigger[i] + (ulong)smaller[i];
                result.Add((uint)carry);
                carry >>= DoubleWordsCollection.BitsInDWord;
                i++;
            }

            while (i < bigger.Length)
            {
                carry += bigger[i];
                result.Add((uint)carry);
                carry >>= DoubleWordsCollection.BitsInDWord;
                i++;
            }

            if (carry != 0) result.Add(1);

            return new BigInt(result.ToArray());
        }

        public static BigInt operator -(BigInt self, BigInt other)
        {
            return self > other ? Subtract(self, other) : new BigInt(new uint[] { 0 });
        }

        private static BigInt Subtract(BigInt bigger, BigInt smaller)
        {

            List<uint> result = bigger._doubleWords.GetDigits;
            int i;
            uint borrow = 0;
            for (i = 0; i < smaller.Length; i++)
            {
                ulong aggregate = result[i];
                ulong toSubtract = (ulong)smaller[i] + (ulong)borrow;
                if (toSubtract > aggregate)
                {
                    borrow = 1;
                    aggregate += (ulong)UInt32.MaxValue + 1;
                    aggregate = (aggregate - toSubtract);
                    result[i] = (uint)(aggregate);
                }
                else
                {
                    borrow = 0;
                    result[i] -= (uint)toSubtract;
                }
            }

            while (borrow != 0)
            {
                if (result[i] == 0)
                {
                    result[i] = UInt32.MaxValue;
                    i++;
                }
                else
                {
                    result[i] = result[i] - 1;
                    borrow = 0;
                }
            }

            while (result[result.Count - 1] == 0)
            {
                result.RemoveAt(result.Count - 1);
            }

            return new BigInt(result.ToArray());
        }

        #region COMPARISON_OPERATORS
        public static bool operator <(BigInt self, BigInt other)
        {
            var areEqual = self.Length == other.Length;
            if (areEqual)
            {
                for (int i = self.Length - 1; i >= 0; i--)
                {
                    if (self[i] < other[i]) return true;
                    if (self[i] > other[i]) return false;
                }

                return false;
            }

            return self.Length < other.Length;
        }

        public static bool operator >(BigInt self, BigInt other)
        {
            var areEqual = self.Length == other.Length;
            if (areEqual)
            {
                for (int i = self.Length - 1; i >= 0; i--)
                {
                    if (self[i] > other[i]) return true;
                    if (self[i] < other[i]) return false;
                }

                return false;
            }

            return self.Length > other.Length;
        }

        public static bool operator <=(BigInt self, BigInt other)
        {
            return !(self > other);
        }

        public static bool operator >=(BigInt self, BigInt other)
        {
            return !(self < other);
        }
        #endregion
    }
}