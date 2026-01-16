namespace TAmeAtCoderLibrary;

public static partial class MathEx
{
    /// <summary>
    /// 等差数列に関する計算を提供します。
    /// </summary>
    public static class ArithmeticSequence
    {
        // --- 非剰余計算 ---

        /// <summary>
        /// 初項 <paramref name="firstTerm"/>、末項 <paramref name="lastTerm"/>、公差 1 の等差数列の和を計算します。
        /// 項数は <paramref name="lastTerm"/> - <paramref name="firstTerm"/> + 1 となります。
        /// </summary>
        /// <remarks>
        /// 計算の途中で <see cref="long"/> 型の範囲を超える場合、オーバーフローが発生する可能性があります (デフォルトの unchecked コンテキスト)。
        /// <paramref name="firstTerm"/> > <paramref name="lastTerm"/> の場合、項数は 0 以下になり、結果は 0 になります。
        /// </remarks>
        /// <param name="firstTerm">初項。</param>
        /// <param name="lastTerm">末項。</param>
        /// <returns>等差数列の和。</returns>
        public static long Sum(long firstTerm, long lastTerm)
        {
            long n = lastTerm - firstTerm + 1;
            if (n <= 0) return 0; // 項数が0以下の場合は和を0とする
                                  // SumFromLastTerm を呼び出す
            return SumFromLastTerm(firstTerm, lastTerm, n); // ★修正: 呼び出すメソッド名を変更
        }

        /// <summary>
        /// 初項 <paramref name="firstTerm"/>、末項 <paramref name="lastTerm"/>、項数 <paramref name="count"/> の等差数列の和を計算します。
        /// </summary>
        /// <remarks>
        /// 計算の途中で <see cref="long"/> 型の範囲を超える場合、オーバーフローが発生する可能性があります (デフォルトの unchecked コンテキスト)。
        /// </remarks>
        /// <param name="firstTerm">初項。</param>
        /// <param name="lastTerm">末項。</param>
        /// <param name="count">項数。負数は指定できません。</param>
        /// <returns>等差数列の和。</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> が負の場合にスローされます。</exception>
        public static long SumFromLastTerm(long firstTerm, long lastTerm, long count) // ★修正: メソッド名を変更
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "項数は 0 以上である必要があります。");
            if (count == 0) return 0;

            // n * (a + l) / 2
            // オーバーフローを少しでも避けるため、偶奇性で先に割る
            long sum_term;
            try
            {
                // checkedブロックでオーバーフローを検出できるようにする (オプション)
                // sum_term = checked(firstTerm + lastTerm);
                sum_term = firstTerm + lastTerm; // checked を使わない場合
            }
            catch (OverflowException)
            {
                // BigInteger を使うなどの代替処理が必要になる場合
                throw; // または BigInteger で再計算
            }


            if (count % 2 == 0)
            {
                // return checked((count / 2) * sum_term); // checked
                return (count / 2) * sum_term;
            }
            else if (sum_term % 2 == 0)
            {
                // return checked(count * (sum_term / 2)); // checked
                return count * (sum_term / 2);
            }
            else
            {
                // count も sum_term も奇数の場合、乗算結果は奇数となり、2で割り切れない。
                // これは整数等差数列の和としては通常発生しないはず。
                // (nが奇数なら a+l = 2a+(n-1)d は偶数になるため)
                // 必要であれば例外をスローするか、警告を出す
                // return checked((count * sum_term) / 2); // checked
                return (count * sum_term) / 2; // 結果は切り捨てられる
            }
        }

        /// <summary>
        /// 初項 <paramref name="firstTerm"/>、公差 <paramref name="commonDifference"/>、項数 <paramref name="count"/> の等差数列の和を計算します。
        /// </summary>
        /// <remarks>
        /// 計算の途中で <see cref="long"/> 型の範囲を超える場合、オーバーフローが発生する可能性があります (デフォルトの unchecked コンテキスト)。
        /// </remarks>
        /// <param name="firstTerm">初項。</param>
        /// <param name="commonDifference">公差。</param>
        /// <param name="count">項数。負数は指定できません。</param>
        /// <returns>等差数列の和。</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> が負の場合にスローされます。</exception>
        public static long SumFromDifference(long firstTerm, long commonDifference, long count) // ★修正: メソッド名を変更
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "項数は 0 以上である必要があります。");
            if (count == 0) return 0;

            // n * (2a + (n - 1)d) / 2
            // オーバーフローを少しでも避けるため、偶奇性で先に割る
            long term1, term2, inner_sum;
            try
            {
                // checkedブロックでオーバーフローを検出できるようにする (オプション)
                // term1 = checked(2 * firstTerm);
                // term2 = checked((count - 1) * commonDifference);
                // inner_sum = checked(term1 + term2);
                term1 = 2 * firstTerm;
                term2 = (count - 1) * commonDifference;
                inner_sum = term1 + term2;
            }
            catch (OverflowException)
            {
                // BigInteger を使うなどの代替処理が必要になる場合
                throw; // または BigInteger で再計算
            }


            if (count % 2 == 0)
            {
                // return checked((count / 2) * inner_sum); // checked
                return (count / 2) * inner_sum;
            }
            else
            {
                // nが奇数の場合、2a + (n-1)d は必ず偶数になる
                // return checked(count * (inner_sum / 2)); // checked
                return count * (inner_sum / 2);
            }
        }

        // --- 剰余計算 ---

        // 剰余計算のヘルパー (負の数にならないように)
        private static long Mod(long value, long divisor)
        {
            long result = value % divisor;
            return result < 0 ? result + divisor : result;
        }

        /// <summary>
        /// 初項 <paramref name="firstTerm"/>、末項 <paramref name="lastTerm"/>、公差 1 の等差数列の和を計算し、<paramref name="divisor"/> で割った余りを求めます。
        /// 項数は <paramref name="lastTerm"/> - <paramref name="firstTerm"/> + 1 となります。
        /// </summary>
        /// <param name="firstTerm">初項。</param>
        /// <param name="lastTerm">末項。</param>
        /// <param name="divisor">剰余を取る数（法）。正の数を指定してください。</param>
        /// <returns>等差数列の和を <paramref name="divisor"/> で割った余り。</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="divisor"/> が 1 未満の場合にスローされます。</exception>
        public static long SumModulo(long firstTerm, long lastTerm, long divisor)
        {
            if (divisor <= 0) throw new ArgumentOutOfRangeException(nameof(divisor), "法は正の数である必要があります。");
            long n = lastTerm - firstTerm + 1;
            if (n <= 0) return 0; // 項数が0以下の場合は和を0とする
                                  // SumModuloFromLastTerm を呼び出す
            return SumModuloFromLastTerm(firstTerm, lastTerm, n, divisor); // ★修正: 呼び出すメソッド名を変更
        }

        /// <summary>
        /// 初項 <paramref name="firstTerm"/>、末項 <paramref name="lastTerm"/>、項数 <paramref name="count"/> の等差数列の和を計算し、<paramref name="divisor"/> で割った余りを求めます。
        /// </summary>
        /// <remarks>
        /// このメソッドは、計算途中のオーバーフローを避けるため、各ステップで剰余を取ります。
        /// 法 <paramref name="divisor"/> が奇数の場合にモジュラ逆数を用いて計算します。
        /// 法が偶数の場合、<see cref="SumModuloFromDifference"/> の使用を推奨します。
        /// </remarks>
        /// <param name="firstTerm">初項。</param>
        /// <param name="lastTerm">末項。</param>
        /// <param name="count">項数。負数は指定できません。</param>
        /// <param name="divisor">剰余を取る数（法）。正の数を指定してください。</param>
        /// <returns>等差数列の和を <paramref name="divisor"/> で割った余り。</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> が負の場合、または <paramref name="divisor"/> が 1 未満の場合にスローされます。</exception>
        /// <exception cref="NotSupportedException">法 <paramref name="divisor"/> が偶数の場合にスローされます。</exception>
        /// <exception cref="InvalidOperationException">法が奇数でも2のモジュラ逆元が存在しない場合にスローされます（通常は発生しません）。</exception>
        public static long SumModuloFromLastTerm(long firstTerm, long lastTerm, long count, long divisor) // ★修正: メソッド名を変更
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "項数は 0 以上である必要があります。");
            if (divisor <= 0) throw new ArgumentOutOfRangeException(nameof(divisor), "法は正の数である必要があります。");
            if (count == 0) return 0;

            long n_mod = Mod(count, divisor);
            long a_mod = Mod(firstTerm, divisor);
            long l_mod = Mod(lastTerm, divisor);

            // S = n * (a + l) / 2 mod divisor
            long term_a_plus_l = Mod(a_mod + l_mod, divisor);

            long result;
            if (count % 2 == 0) // n が偶数
            {
                // n/2 * (a+l) mod divisor
                long n_half_mod = Mod(count / 2, divisor);
                result = Mod(n_half_mod * term_a_plus_l, divisor);
            }
            else // n が奇数
            {
                // n * (a+l)/2 mod divisor
                // 法が奇数なら ModInverse(2) を使う
                if (divisor % 2 != 0)
                {
                    long inv2 = ModInverse(2, divisor);
                    if (inv2 == -1) throw new InvalidOperationException($"2のモジュラ逆元(法 {divisor})が存在しません。");
                    long numerator = Mod(n_mod * term_a_plus_l, divisor);
                    result = Mod(numerator * inv2, divisor);
                }
                else // 法が偶数
                {
                    // このメソッドでは安全に計算できないため例外をスロー
                    throw new NotSupportedException("法が偶数の場合、SumModuloFromLastTerm の使用は推奨されません。SumModuloFromDifference を使用してください。");
                }
            }
            return result;
        }


        /// <summary>
        /// 初項 <paramref name="firstTerm"/>、公差 <paramref name="commonDifference"/>、項数 <paramref name="count"/> の等差数列の和を計算し、<paramref name="divisor"/> で割った余りを求めます。
        /// </summary>
        /// <remarks>
        /// このメソッドは、計算途中のオーバーフローを避けるため、各ステップで剰余を取ります。
        /// 法 <paramref name="divisor"/> が偶数の場合でも正確に動作します。
        /// </remarks>
        /// <param name="firstTerm">初項。</param>
        /// <param name="commonDifference">公差。</param>
        /// <param name="count">項数。負数は指定できません。</param>
        /// <param name="divisor">剰余を取る数（法）。正の数を指定してください。</param>
        /// <returns>等差数列の和を <paramref name="divisor"/> で割った余り。</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> が負の場合、または <paramref name="divisor"/> が 1 未満の場合にスローされます。</exception>
        public static long SumModuloFromDifference(long firstTerm, long commonDifference, long count, long divisor) // ★修正: メソッド名を変更
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count), "項数は 0 以上である必要があります。");
            if (divisor <= 0) throw new ArgumentOutOfRangeException(nameof(divisor), "法は正の数である必要があります。");
            if (count == 0) return 0;

            long n_mod = Mod(count, divisor);
            long a_mod = Mod(firstTerm, divisor);
            long d_mod = Mod(commonDifference, divisor);

            // S = n * (2a + (n - 1)d) / 2 mod divisor
            // n の偶奇性に基づいて /2 を処理する
            long term_2a = Mod(2 * a_mod, divisor);
            // count - 1 が負にならないように注意 (count=0 は上で処理済み)
            long n_minus_1_mod = Mod(count - 1, divisor);
            long term_n_minus_1_d = Mod(n_minus_1_mod * d_mod, divisor);
            long inner_sum_mod = Mod(term_2a + term_n_minus_1_d, divisor);

            long result;
            if (count % 2 == 0) // n が偶数
            {
                // n/2 * (2a + (n-1)d) mod divisor
                long n_half_mod = Mod(count / 2, divisor);
                result = Mod(n_half_mod * inner_sum_mod, divisor);
            }
            else // n が奇数
            {
                // n * (a + (n-1)/2 * d) mod divisor
                // (n-1)/2 を計算
                long n_minus_1_half_mod = Mod((count - 1) / 2, divisor);
                long term2_half = Mod(n_minus_1_half_mod * d_mod, divisor);
                long inner_sum_half_mod = Mod(a_mod + term2_half, divisor);
                result = Mod(n_mod * inner_sum_half_mod, divisor);
            }

            return result;
        }

        // --- ModInverse の実装例 (法が素数の場合) ---
        private static long ModInverse(long value, long divisor)
        {
            long g = ExtendedGCD(value, divisor, out long x, out long y);
            if (g != 1) return -1;
            return Mod(x, divisor);
        }

        // --- ExtendedGCD の実装例 ---
        private static long ExtendedGCD(long a, long b, out long x, out long y)
        {
            if (b == 0)
            {
                x = 1;
                y = 0;
                return a;
            }
            long gcd = ExtendedGCD(b, a % b, out long x1, out long y1);
            x = y1;
            y = x1 - (a / b) * y1;
            return gcd;
        }
    }
}