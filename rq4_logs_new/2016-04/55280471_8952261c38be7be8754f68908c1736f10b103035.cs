using System;
using System.Collections;
using System.Text;
using System.Text.RegularExpressions;

namespace SExpressionParser
{
    /// <summary>
    /// S式をダンプするクラス
    /// </summary>
    public static class Dump
    {
        /// <summary>
        /// S式のダンプを返す
        /// </summary>
        /// <param name="contents">S式のパース結果</param>
        /// <returns>ダンプ結果</returns>
        public static string Dumps(dynamic contents)
        {
            var buffer = new StringBuilder();
            
            // 各要素を文字列に変換
            if (contents is IList)
            {
                buffer.Append("(");
                for (int i = 0; i < contents.Count; i++)
                {
                    buffer.Append(Dumps(contents[i]));
                    if (i + 1 != contents.Count)
                        buffer.Append(" ");
                }
                buffer.Append(")");
            }
            else if (contents is int || contents is double)
                buffer.Append(contents.ToString());
            else if (contents is string)
            {
                buffer.Append('"');
                buffer.Append(ConvertControlCharacter(contents.ToString()));
                buffer.Append('"');
            }

            return buffer.ToString();
        }

        /// <summary>
        /// 制御文字を対応する文字列に置換する
        /// </summary>
        /// <param name="str">置換したい文字列</param>
        /// <returns>制御文字を対応する文字列に置換した結果</returns>
        private static string ConvertControlCharacter(string str)
        {
            // 制御文字の置き換え対象のリスト
            var ctrlList = new[]
            {"NUL", "SOH", "STX", "ETX", "EOT", "ENQ", "ACK", "BEL",
             "BS",  "HT",  "LF",  "VT",  "NP",  "CR",  "SO",  "SI",
             "DLE", "DC1", "DC2", "DC3", "DC4", "NAK", "SYN", "ETB",
             "CAN", "EM",  "SUB", "ESC", "FS",  "GS",  "RS",  "US" };

            // 文字列中の制御文字を上のリストの文字に置き換える
            // 文字列中の制御文字を上のリストの文字に置き換える
            return Regex.Replace(
                str,
                @"\p{Cc}",
                s =>
                {
                    if (ctrlList.Length > s.Value[0])
                        return "[" + ctrlList[s.Value[0]] + "]";
                    else
                        return String.Format("<{0:X2}>", (byte) s.Value[0]);
                });
        }
    }
}