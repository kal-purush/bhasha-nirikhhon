using Org.BouncyCastle.Asn1.Ocsp;
using System;
using System.ComponentModel;


namespace Standard.Tool.Platform.Common.Helper;

/// <summary>
/// 枚举的扩展方法
/// </summary>
public static class EnumExtensions
{
    public static string Code(this Enum e)
    {
        return e.ToString();
    }

    public static string Ordinal(this Enum e)
    {
        int a = Convert.ToInt32(e);
        return a.ToString();
    }

    /// <summary>
    /// 获取枚举值的描述信息
    /// </summary>
    /// <typeparam name="TSource"></typeparam>
    /// <param name="source"></param>
    /// <returns></returns>
    public static string GetDescription<TSource>(this TSource source)
        where TSource : struct
    {
        var field = source.GetType().GetField(source.ToString());
        if (field != null)
        {
            var descript = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (descript is { Length: > 0 })
            {
                return (descript[0] as DescriptionAttribute).Description;
            }
        }
        return source.ToString();
    }
}