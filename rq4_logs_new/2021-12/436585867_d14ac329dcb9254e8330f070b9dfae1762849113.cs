using System.Runtime.InteropServices;

namespace XIVComboExpandedPlugin
{
    /// <summary>
    /// Internal cooldown data.
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    internal struct CooldownData
    {
        /// <summary>
        /// A value indicating whether the action is on cooldown.
        /// </summary>
        [FieldOffset(0x0)]
        public bool IsCooldown;

        /// <summary>
        /// Action ID on cooldown.
        /// </summary>
        [FieldOffset(0x4)]
        public uint ActionID;

        /// <summary>
        /// The elapsed cooldown time.
        /// </summary>
        [FieldOffset(0x8)]
        public float CooldownElapsed;

        /// <summary>
        /// The total cooldown time.
        /// </summary>
        [FieldOffset(0xC)]
        public float CooldownTotal;

        /// <summary>
        /// Gets the cooldown time remaining.
        /// </summary>
        public float CooldownRemaining => this.IsCooldown ? this.CooldownTotal - this.CooldownElapsed : 0;
    }
}