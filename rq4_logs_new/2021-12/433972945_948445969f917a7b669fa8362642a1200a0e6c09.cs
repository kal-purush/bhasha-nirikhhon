namespace AocNetLib
{
    public class Day18
    {
        public string Solve(string v)
        {
            var nums = ParseInput(v);
            var result = nums.First();
            if (nums.Length == 1)
            {
                result.Reduce();
            }
            else
            {
                for (int i = 1; i < nums.Length; i++)
                {
                    result = new Number { Ref1 = result, Ref2 = nums[i] };
                    result.Reduce();
                }
            }
            Console.WriteLine(result);
            return result.GetMagnitude().ToString();
        }

        public string Solve2(string v)
        {
            var nums = ParseInput(v);
            int max = 0;
            int maxi = -1, maxj = -1;
            Number maxResult = null;
            for (int i = 0; i < nums.Length; i++)
            {
                for (int j = 0; j < nums.Length; j++)
                {
                    if(i!=j)
                    {
                        var result = new Number { Ref1 = nums[i], Ref2 = nums[j] };
                        result.Reduce();
                        var magn = result.GetMagnitude();
                        if (magn > max)
                        {
                            max = magn;
                            maxi = i;
                            maxj = j;
                            maxResult = result;
                        }
                    }
                }
            }
            Console.WriteLine(maxResult);
            return max.ToString();
        }

        private Number[] ParseInput(string input)
        {
            var lines = input.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
            var nums = lines.Select(ParseLine).ToArray();
            return nums;
        }

        private Number ParseLine(string line)
        {
            int idx = 0;
            return ParseLineRecursive(line, ref idx);
        }

        private Number ParseLineRecursive(string line, ref int idx)
        {
            Number num = new Number();
            while(idx < line.Length)
            {
                switch(line[idx])
                {
                    case '[':
                        idx++;
                        num.Ref1 = ParseLineRecursive(line, ref idx);
                        break;
                    case ',':
                        idx++;
                        num.Ref2 = ParseLineRecursive(line, ref idx);
                        break;
                    case ']':
                        idx++;
                        return num;
                    default:
                        int len = 1;
                        while (char.IsNumber(line[idx + len])) len++;
                        num.IsValue = true;
                        num.Value = int.Parse(line.Substring(idx, len));
                        idx+=len;
                        return num;
                }
            }
            throw new Exception("Unexpected end of string");
        }

        class Number
        {
            private Number ref1;
            private Number ref2;

            public bool IsValue { get; set; }
            public int Value { get; set; }
            public Number Ref1 { get => ref1; set { ref1 = value; if (ref1 != null) ref1.Top = this; } }
            public Number Ref2 { get => ref2; set { ref2 = value; if (ref2 != null) ref2.Top = this; } }
            public Number Top { get; private set; }

            public override string ToString()
            {
                return IsValue ? Value.ToString() : $"[{Ref1},{Ref2}]";
            }

            internal int GetMagnitude()
            {
                return IsValue ? Value : (3 * Ref1.GetMagnitude() + 2 * Ref2.GetMagnitude());
            }

            internal void Reduce()
            {
                while (ReduceStep()) ;
            }

            private bool ReduceStep()
            {
                if (TryExplode(0)) return true;
                if (TrySplit()) return true;
                return false;
            }

            private bool TryExplode(int depth)
            {
                if (IsValue) return false;
                if (depth >= 4)
                {
                    if (Ref1.IsValue == false || Ref2.IsValue == false) throw new ArgumentException("Exploding non-value pair");
                    var left = FindLeft();
                    if(left != null)
                    {
                        left.Value += Ref1.Value;
                    }
                    var right = FindRight();
                    if (right != null)
                    {
                        right.Value += Ref2.Value;
                    }
                    IsValue = true;
                    Value = 0;
                    Ref1 = null;
                    Ref2 = null;
                    return true;
                }
                if(Ref1.TryExplode(depth + 1)) return true;
                if(Ref2.TryExplode(depth + 1)) return true;
                return false;
            }

            private Number FindRight()
            {
                if (Top == null) return null;
                if (Top.Ref1 == this) return Top.Ref2.LeftValue();
                return Top.FindRight();
            }

            private Number LeftValue()
            {
                if (IsValue) return this;
                return Ref1.LeftValue();
            }

            private Number FindLeft()
            {
                if (Top == null) return null;
                if (Top.Ref2 == this) return Top.Ref1.RightValue();
                return Top.FindLeft();
            }

            private Number RightValue()
            {
                if (IsValue) return this;
                return Ref2.RightValue();
            }

            private bool TrySplit()
            {
                if (IsValue)
                {
                    if (Value < 10) return false;
                    IsValue = false;
                    Ref1 = new Number { IsValue = true, Value = Value / 2 };
                    Ref2 = new Number { IsValue = true, Value = (Value + 1) / 2 };
                    return true;
                }
                if (Ref1.TrySplit()) return true;
                if (Ref2.TrySplit()) return true;
                return false;
            }
        }
    }
}