using System.Text;

namespace TAmeAtCoderLibrary.Utilities;

/// <summary>
/// 数値変換や数値操作に関する静的ユーティリティメソッドを提供します。
/// </summary>
public static class Numeric
{
    // --- 基数変換 ---

    /// <summary>
    /// 10進数の非負整数を指定された基数 (2以上) の文字列表現に変換します。
    /// </summary>
    /// <param name="decimalValue">変換する非負の10進数。</param>
    /// <param name="targetBase">変換先の基数 (2以上)。</param>
    /// <returns>指定された基数に変換された文字列。</returns>
    /// <exception cref="ArgumentOutOfRangeException">decimalValueが負の場合、またはtargetBaseが2未満の場合にスローされます。</exception>
    public static string ConvertToBase(long decimalValue, int targetBase) // targetBaseはintで十分な場合が多い
    {
        if (decimalValue < 0)
            throw new ArgumentOutOfRangeException(nameof(decimalValue), "Input value must be non-negative.");
        if (targetBase < 2)
            throw new ArgumentOutOfRangeException(nameof(targetBase), "Target base must be 2 or greater.");
        if (decimalValue == 0L) return "0";

        // 10以上の基数でA-Zなどを使う場合は、より複雑な実装が必要
        // ここでは targetBase <= 10 を想定した簡易版 (元のコードに合わせた挙動)
        if (targetBase > 36) // 例: 36進数まで対応する場合など
            throw new ArgumentOutOfRangeException(nameof(targetBase), "Target base greater than 36 is not supported by this simple implementation.");

        var resultBuilder = new StringBuilder();
        var value = decimalValue;

        while (value > 0)
        {
            long remainder = value % targetBase;
            // 10以上の場合は文字に変換する必要がある (A-Zなど)
            char digitChar = (remainder < 10)
                           ? (char)('0' + remainder)
                           : (char)('A' + remainder - 10); // 10->A, 11->B ...
            resultBuilder.Insert(0, digitChar); // 先頭に追加していく
            value /= targetBase;
        }

        return resultBuilder.ToString();
    }

    // ConvertToBinaryはConvertToBase(value, 2) を使って実装可能だが、
    // int[] を返す要件があるため別途実装する。ロジック共通化は可能。

    private static Stack<int> GetDigitsInBase(long value, int targetBase)
    {
        if (value < 0) throw new ArgumentOutOfRangeException(nameof(value), "Input value must be non-negative.");
        if (targetBase < 2) throw new ArgumentOutOfRangeException(nameof(targetBase), "Target base must be 2 or greater.");

        if (value == 0L) return new Stack<int>(new[] { 0 }); // 0の場合

        var stack = new Stack<int>();
        while (value > 0L)
        {
            // targetBaseが大きい場合、remainderがintの範囲を超える可能性に注意 (longなら大丈夫)
            var remainder = value % targetBase;
            if (remainder >= int.MaxValue) throw new OverflowException("Remainder exceeds Int32.MaxValue."); // 安全策
            stack.Push((int)remainder);
            value /= targetBase;
        }
        return stack;
    }


    /// <summary>
    /// 10進数の非負整数を2進数の各桁を表す整数配列に変換します。
    /// </summary>
    /// <param name="decimalValue">変換する非負の10進数。</param>
    /// <returns>2進数の各桁を表す整数配列 (最上位桁が最初)。</returns>
    /// <exception cref="ArgumentOutOfRangeException">decimalValueが負の場合。</exception>
    public static int[] ConvertToBinary(long decimalValue)
    {
        // 0の扱い: 元のコードに合わせて {0} を返す
        if (decimalValue == 0) return new int[] { 0 };
        // 負数はエラー
        if (decimalValue < 0) throw new ArgumentOutOfRangeException(nameof(decimalValue), "Input value must be non-negative.");

        var stack = GetDigitsInBase(decimalValue, 2);
        return stack.ToArray(); // Stack<T>.ToArray() は Push した順の逆 (正しい順序)
    }


    /// <summary>
    /// 10進数の非負整数を2進数に変換し、指定された最小桁数になるよう先頭を0で埋めます。
    /// </summary>
    /// <param name="decimalValue">変換する非負の10進数。</param>
    /// <param name="minDigits">結果の最小桁数 (非負)。</param>
    /// <returns>2進数の各桁を表す整数配列（指定された最小桁数以上）。</returns>
    /// <exception cref="ArgumentOutOfRangeException">decimalValueが負の場合、またはminDigitsが負の場合。</exception>
    public static int[] ConvertToBinary(long decimalValue, int minDigits)
    {
        if (decimalValue < 0)
            throw new ArgumentOutOfRangeException(nameof(decimalValue), "Input value must be non-negative.");
        if (minDigits < 0)
            throw new ArgumentOutOfRangeException(nameof(minDigits), "Minimum digits cannot be negative.");

        if (decimalValue == 0L) return new int[minDigits]; // 0埋め配列

        var stack = GetDigitsInBase(decimalValue, 2);
        var binaryDigits = stack.ToArray(); // 正しい順序

        if (binaryDigits.Length >= minDigits)
        {
            return binaryDigits;
        }

        // 足りない分を0で埋める
        var paddedResult = new int[minDigits];
        int leadingZeros = minDigits - binaryDigits.Length;

        for (int i = 0; i < binaryDigits.Length; i++)
        {
            paddedResult[leadingZeros + i] = binaryDigits[i];
        }

        return paddedResult;
    }


    /// <summary>
    /// 指定された基数 (2以上36以下) の文字列表現を10進数の値に変換します。
    /// </summary>
    /// <param name="numberString">基数表現の文字列 (数字0-9、文字A-Zまたはa-z)。</param>
    /// <param name="sourceBase">入力文字列の基数 (2以上36以下)。</param>
    /// <returns>変換された10進数の値。</returns>
    /// <exception cref="ArgumentNullException">numberStringがnullの場合。</exception>
    /// <exception cref="ArgumentException">numberStringが空の場合、または不正な文字を含む場合。</exception>
    /// <exception cref="ArgumentOutOfRangeException">sourceBaseが2未満または36より大きい場合。</exception>
    /// <exception cref="OverflowException">結果がlongの範囲を超える場合。</exception>
    public static long ConvertToDecimal(string numberString, int sourceBase) // sourceBaseもintで十分
    {
        if (numberString == null)
            throw new ArgumentNullException(nameof(numberString));
        if (string.IsNullOrWhiteSpace(numberString)) // 空白のみもエラーとする
            throw new ArgumentException("Input string cannot be empty or whitespace.", nameof(numberString));
        if (sourceBase < 2 || sourceBase > 36)
            throw new ArgumentOutOfRangeException(nameof(sourceBase), "Source base must be between 2 and 36.");

        long decimalValue = 0L;
        long power = 1L;

        // checked コンテキストでオーバーフローを検出
        checked
        {
            for (int i = numberString.Length - 1; i >= 0; i--)
            {
                char c = numberString[i];
                int digit;

                if (c >= '0' && c <= '9')
                    digit = c - '0';
                else if (c >= 'A' && c <= 'Z')
                    digit = c - 'A' + 10;
                else if (c >= 'a' && c <= 'z') // 小文字も許容する場合
                    digit = c - 'a' + 10;
                else
                    throw new ArgumentException($"Invalid character '{c}' found in the input string for base {sourceBase}.", nameof(numberString));

                if (digit >= sourceBase)
                    throw new ArgumentException($"Digit '{c}' (value {digit}) is invalid for base {sourceBase}.", nameof(numberString));

                decimalValue += digit * power;

                // 次の桁の累乗を計算 (最後の桁では不要)
                if (i > 0)
                {
                    // power * sourceBase が long の範囲を超えるかチェック
                    if (power > long.MaxValue / sourceBase) // 事前チェックの方が安全な場合も
                        throw new OverflowException("Intermediate power value exceeds Long.MaxValue.");
                    power *= sourceBase;
                }
            }
        } // End checked

        return decimalValue;
    }


    /// <summary>
    /// 指定された基数 (2以上) の各桁を表す整数配列を10進数の値に変換します。
    /// 各要素は 0 以上 sourceBase 未満である必要があります。
    /// </summary>
    /// <param name="digits">基数表現の各桁を表す整数配列 (最上位桁が最初)。</param>
    /// <param name="sourceBase">入力配列の基数 (2以上)。</param>
    /// <returns>変換された10進数の値。</returns>
    /// <exception cref="ArgumentNullException">digitsがnullの場合。</exception>
    /// <exception cref="ArgumentException">digitsが空の場合、または不正な桁の値を含む場合。</exception>
    /// <exception cref="ArgumentOutOfRangeException">sourceBaseが2未満の場合。</exception>
    /// <exception cref="OverflowException">結果がlongの範囲を超える場合。</exception>
    public static long ConvertToDecimal(int[] digits, int sourceBase) // sourceBaseもint
    {
        if (digits == null)
            throw new ArgumentNullException(nameof(digits));
        if (digits.Length == 0)
            throw new ArgumentException("Input array cannot be empty.", nameof(digits));
        if (sourceBase < 2)
            throw new ArgumentOutOfRangeException(nameof(sourceBase), "Source base must be 2 or greater.");

        long decimalValue = 0L;
        long power = 1L;

        checked
        {
            // 配列の最後（最下位桁）から処理
            for (int i = digits.Length - 1; i >= 0; i--)
            {
                int digit = digits[i];
                if (digit < 0 || digit >= sourceBase)
                    throw new ArgumentException($"Invalid digit '{digit}' found in the input array for base {sourceBase}.", nameof(digits));

                // decimalValue += (long)digit * power; // キャストは安全だが念のため
                // より安全にするなら
                if (power > long.MaxValue / digit && digit != 0) // digitが0なら乗算は0なのでOK
                    throw new OverflowException("Intermediate multiplication result exceeds Long.MaxValue.");
                long term = (long)digit * power;
                if (decimalValue > long.MaxValue - term)
                    throw new OverflowException("Final decimal value exceeds Long.MaxValue.");
                decimalValue += term;


                if (i > 0)
                {
                    if (power > long.MaxValue / sourceBase)
                        throw new OverflowException("Intermediate power value exceeds Long.MaxValue.");
                    power *= sourceBase;
                }
            }
        } // End checked

        return decimalValue;
    }

    // --- 数値操作 ---

    /// <summary>
    /// 非負整数を各桁の数字を格納したリストに分解します。
    /// </summary>
    /// <param name="number">分解する非負の数値。</param>
    /// <returns>各桁の数字を格納した整数リスト (最上位桁が最初)。</returns>
    /// <exception cref="ArgumentOutOfRangeException">numberが負の場合。</exception>
    public static List<int> ToIntList(long number)
    {
        if (number < 0)
            throw new ArgumentOutOfRangeException(nameof(number), "Input number must be non-negative.");
        if (number == 0) return new List<int> { 0 }; // 0の扱い

        // LinkedListを使った代替案
        var digits = new LinkedList<int>();
        var current = number;
        while (current > 0)
        {
            digits.AddFirst((int)(current % 10)); // 先頭に追加
            current /= 10;
        }
        return digits.ToList();
    }

    // Common.Power への依存をなくすため、整数冪乗を内部で計算する例
    // (簡易版、オーバーフローチェックが必要な場合がある)
    private static long IntegerPower(long baseVal, int exponent)
    {
        if (exponent < 0) throw new ArgumentOutOfRangeException(nameof(exponent), "Exponent cannot be negative.");
        if (exponent == 0) return 1L;
        if (baseVal == 0) return 0L;

        long result = 1L;
        long currentPower = baseVal;
        int exp = exponent;

        // より効率的な二分累乗法 (Exponentiation by squaring) もある
        // checked コンテキストでオーバーフロー検出を推奨
        checked
        {
            while (exp > 0)
            {
                if ((exp % 2) == 1)
                    result *= currentPower;
                currentPower *= currentPower;
                exp /= 2;
            }
        }
        return result;
    }

    /// <summary>
    /// 指定した数値の特定の位置（右から1ベース）の桁を新しい数字に置き換えます。
    /// </summary>
    /// <param name="number">対象の非負整数。</param>
    /// <param name="newDigit">新しい数字 (0-9)。</param>
    /// <param name="position">変更する桁の位置（1ベース、右から数えて）。</param>
    /// <returns>指定した桁を変更した新しい数値。</returns>
    /// <exception cref="ArgumentOutOfRangeException">numberが負の場合、newDigitが0-9の範囲外の場合、positionが1未満または桁数を超える場合。</exception>
    public static long SetDigitAtPosition(long number, int newDigit, int position) // メソッド名変更, newDigitはintで十分
    {
        if (number < 0) throw new ArgumentOutOfRangeException(nameof(number), "Input number must be non-negative.");
        if (newDigit < 0 || newDigit > 9) throw new ArgumentOutOfRangeException(nameof(newDigit), "New digit must be between 0 and 9.");
        if (position < 1) throw new ArgumentOutOfRangeException(nameof(position), "Position must be 1 or greater.");

        long powerOf10Pos;
        long powerOf10PosMinus1;

        try
        {
            // checkedでオーバーフローを検出
            checked
            {
                powerOf10Pos = IntegerPower(10, position);
                powerOf10PosMinus1 = IntegerPower(10, position - 1);
            }
        }
        catch (OverflowException)
        {
            // 冪乗計算でオーバーフローした場合、指定位置が大きすぎる可能性がある
            throw new ArgumentOutOfRangeException(nameof(position), "Position results in calculation overflow.");
        }


        // 指定位置が存在するか簡易チェック (より厳密には桁数を数える)
        if (number < powerOf10PosMinus1 && number > 0) // number=0の場合はposition=1のみ有効
            throw new ArgumentOutOfRangeException(nameof(position), $"Position {position} is out of range for the number {number}.");
        if (number == 0 && position > 1)
            throw new ArgumentOutOfRangeException(nameof(position), $"Position {position} is out of range for the number 0.");


        long upperPart = (number / powerOf10Pos) * powerOf10Pos; // 上位部分
        long lowerPart = number % powerOf10PosMinus1; // 下位部分
        long newDigitValue = (long)newDigit * powerOf10PosMinus1; // 新しい桁の値

        // 結果がオーバーフローしないかチェック
        long result;
        checked
        {
            result = upperPart + lowerPart + newDigitValue;
        }
        return result;
    }


    /// <summary>
    /// [特殊要件] 指定した数値の2つの位置（右から1ベース）の桁を入れ替えます。
    /// ただし、入れ替え対象の桁のどちらかが9の場合のみ処理を実行します。
    /// </summary>
    /// <param name="number">対象の非負整数。</param>
    /// <param name="position1">入れ替える1つ目の桁の位置（1ベース）。</param>
    /// <param name="position2">入れ替える2つ目の桁の位置（1ベース）。</param>
    /// <returns>桁を入れ替えた新しい数値。どちらの桁も9でない場合は元の数値を返します。</returns>
    /// <exception cref="ArgumentOutOfRangeException">numberが負の場合、position1またはposition2が不正な場合。</exception>
    /// <exception cref="ArgumentException">position1とposition2が同じ場合。</exception>
    public static long SwapDigitsIfNinePresent(long number, int position1, int position2) // メソッド名変更
    {
        if (number < 0) throw new ArgumentOutOfRangeException(nameof(number), "Input number must be non-negative.");
        if (position1 < 1) throw new ArgumentOutOfRangeException(nameof(position1), "Position must be 1 or greater.");
        if (position2 < 1) throw new ArgumentOutOfRangeException(nameof(position2), "Position must be 1 or greater.");
        if (position1 == position2) throw new ArgumentException("Positions cannot be the same.", nameof(position1)); // 同じ位置は無意味

        // 桁の値を取得 (オーバーフローや範囲チェックはSetDigitAtPositionに任せる手もあるが、ここで簡易チェックも可)
        long powerP1Minus1, powerP2Minus1;
        int digit1, digit2;
        try
        {
            checked
            {
                powerP1Minus1 = IntegerPower(10, position1 - 1);
                powerP2Minus1 = IntegerPower(10, position2 - 1);
                digit1 = (int)(number / powerP1Minus1 % 10L);
                digit2 = (int)(number / powerP2Minus1 % 10L);
            }
        }
        catch (OverflowException)
        {
            throw new ArgumentOutOfRangeException($"Position {Math.Max(position1, position2)} results in calculation overflow.");
        }
        // ここで桁数のチェックを追加するとより堅牢

        // 条件判定: どちらかの桁が9でなければ元の値を返す
        if (digit1 != 9 && digit2 != 9)
        {
            return number;
        }

        // SetDigitAtPosition を使って入れ替え
        // 注意: 一度変更すると元の桁の値が変わるため、順番に実行する
        long intermediateNumber = SetDigitAtPosition(number, digit2, position1);
        long finalNumber = SetDigitAtPosition(intermediateNumber, digit1, position2);

        return finalNumber;
    }
}