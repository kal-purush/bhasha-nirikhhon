using Engine.Models.Boards;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;

namespace Engine.Services.Bits
{
    public class AmdBitService : BitServiceBase
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override byte BitScanForward(BitBoard b)
        {
            return (byte)ArmBase.Arm64.LeadingZeroCount(ArmBase.Arm64.ReverseElementBits(b.AsValue()));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int Count(BitBoard b)
        {
            ulong value = b.AsValue();
            const ulong c1 = 0x_55555555_55555555ul;
            const ulong c2 = 0x_33333333_33333333ul;
            const ulong c3 = 0x_0F0F0F0F_0F0F0F0Ful;
            const ulong c4 = 0x_01010101_01010101ul;

            value -= value >> 1 & c1;
            value = (value & c2) + (value >> 2 & c2);
            value = (value + (value >> 4) & c3) * c4 >> 56;

            return (int)value;
        }
    }
}