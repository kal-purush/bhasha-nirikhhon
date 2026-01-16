using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LCDUtils
{
    public static class ILI9341Constants
    {
        public const byte NoOp = 0x00;
        public const byte SoftwareReset = 0x01;
        public const byte EnterSleepMode = 0x10;
        public const byte SleepModeOut = 0x11;
        public const byte PartialModeOn = 0x12;
        public const byte NormalDisplayOn = 0x13;
        public const byte DisplayInversionOff = 0x20;
        public const byte DisplayInversionOn = 0x21;
        public const byte GammaSet = 0x26;
        public const byte DisplayOff = 0x28;
        public const byte DisplayOn = 0x29;
        public const byte ColumnAddressSet = 0x2A;
        public const byte PageAddressSet = 0x2B;
        public const byte MemoryWrite = 0x2C;
        public const byte ColorSet = 0x2D;
        public const byte PartialArea = 0x30;
        public const byte VerticalScrollingDefinition = 0x33;
        public const byte TearingEffectLineOff = 0x34;
        public const byte TearingEffectLineOn= 0x35;
        public const byte MemoryAccessControl = 0x36;
        public const byte VerticalScrollingStartAddress = 0x37;
        public const byte IdleModeOff = 0x38;
        public const byte IdleModeOn = 0x39;
        public const byte PixelFormatSet = 0x3A;
        public const byte WriteMemoryContinue = 0x3C;
        public const byte ReadMemoryContinue = 0x3E;
        public const byte SetTearScanline = 0x44;
        public const byte GetScanline = 0x45;
        public const byte WriteDisplayBrightess = 0x51;
        public const byte WriteCTRLDisplay = 0x53;
        public const byte WriteContentAdaptiveBrightnessControl = 0x55;
        public const byte WriteCABCMinimumBrightness = 0x5E;
        public const byte RGBInterfaceSignalControl = 0xB0;
        public const byte FrameControlNormalMode = 0xB1;
        public const byte FrameControlIdleMode = 0xB2;
        public const byte FrameControlPartialMode = 0xB3;
        public const byte DisplayInversionControl = 0xB4;
        public const byte DisplayFunctionControl = 0xB6;
        public const byte EntryModeSet = 0xB7;
        public const byte BacklightControl1 = 0xB8;
        public const byte BacklightControl2 = 0xB9;
        public const byte BacklightControl3 = 0xBA;
        public const byte BacklightControl4 = 0xBB;
        public const byte BacklightControl5 = 0xBC;
        public const byte BacklightControl7 = 0xBE;
        public const byte BacklightControl8 = 0xBF;
        public const byte PowerControl1 = 0xC0;
        public const byte PositiveGammaCorrection = 0xE0;
        public const byte NegativeGammaCorrection = 0xE1;
        public const byte DigitalGammaControl = 0xE2;
        public const byte DigitalGammaControl2 = 0xE3;
    }
}