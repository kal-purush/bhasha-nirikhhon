namespace Sandbox.Structures;

// https://qiita.com/keymoon/items/11fac5627672a6d6a9f6
public class RollingHash
{
    private const ulong Mask30 = (1UL << 30) - 1;
    private const ulong Mask31 = (1UL << 31) - 1;
    private const ulong Modulo = (1UL << 61) - 1;
    private const ulong Positivizer = Modulo * ((1UL << 3) - 1);
    private const int MaxPowerLength = (int)1e5;
    private static readonly ulong[] Powers = new ulong[MaxPowerLength + 1];
    private static readonly ulong Base;

    static RollingHash()
    {
        Base = (ulong)new Random().Next(1 << 8, 1 << 30);
        Powers[0] = 1;
        for (var i = 0; i + 1 < Powers.Length; i++)
        {
            Powers[i + 1] = CalcModulo(Multiply(Powers[i], Base));
        }
    }

    private readonly ulong[] _hash;

    public RollingHash(ReadOnlySpan<char> s)
    {
        _hash = new ulong[s.Length + 1];
        for (var i = 0; i < s.Length; i++)
        {
            _hash[i + 1] = CalcModulo(Multiply(_hash[i], Base) + s[i]);
        }
    }

    public ulong Slice(int l, int r)
    {
        return CalcModulo(_hash[r] + Positivizer - Multiply(_hash[l], Powers[r - l]));
    }

    private static ulong Multiply(ulong a, ulong b)
    {
        var au = a >> 31;
        var ad = a & Mask31;
        var bu = b >> 31;
        var bd = b & Mask31;
        var m = ad * bu + au * bd;
        var mu = m >> 30;
        var md = m & Mask30;
        return ((au * bu) << 1) + mu + (md << 31) + ad * bd;
    }

    private static ulong CalcModulo(ulong v)
    {
        var vu = v >> 61;
        var vd = v & Modulo;
        var x = vu + vd;
        return x < Modulo ? x : x - Modulo;
    }
}