using System;

namespace MapEngine.Extensions
{
    public static class DegreesEx
    {
        public static float ToRadians(this float val)
        {
            return (float) (Math.PI / 180) * val;
        }
    }
}