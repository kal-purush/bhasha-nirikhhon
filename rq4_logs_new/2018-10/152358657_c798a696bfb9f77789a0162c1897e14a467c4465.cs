namespace Smelt.Tests.Data.RomMakeup
{
    using System;
    using Shouldly;
    using Smelt.Data;

    public class WhenParsingRomMakeup
    {
        public void ShouldParseLoRom()
        {
            byte input = 0x20;
            var makeup = RomMakeup.Parse(input);
            makeup.Mapping.ShouldBe(Mapping.LoRom);
            makeup.ExHiRom.ShouldBe(false);
            makeup.ExLoRom.ShouldBe(false);
            makeup.FastRom.ShouldBe(false);
            makeup.Sa1.ShouldBe(false);
        }

        public void ShouldParseHiRom()
        {
            byte input = 0x21;
            var makeup = RomMakeup.Parse(input);
            makeup.Mapping.ShouldBe(Mapping.HiRom);
            makeup.ExHiRom.ShouldBe(false);
            makeup.ExLoRom.ShouldBe(false);
            makeup.FastRom.ShouldBe(false);
            makeup.Sa1.ShouldBe(false);
        }

        public void ShouldParseExHiRom()
        {
            byte input = 0x25;
            var makeup = RomMakeup.Parse(input);
            makeup.Mapping.ShouldBe(Mapping.HiRom);
            makeup.ExHiRom.ShouldBe(true);
            makeup.ExLoRom.ShouldBe(false);
            makeup.FastRom.ShouldBe(false);
            makeup.Sa1.ShouldBe(false);
        }

        public void ShouldParseExLoRom()
        {
            byte input = 0x22;
            var makeup = RomMakeup.Parse(input);
            makeup.Mapping.ShouldBe(Mapping.LoRom);
            makeup.ExHiRom.ShouldBe(false);
            makeup.ExLoRom.ShouldBe(true);
            makeup.FastRom.ShouldBe(false);
            makeup.Sa1.ShouldBe(false);
        }

        public void ShouldParseLoRomFastRom()
        {
            byte input = 0x30;
            var makeup = RomMakeup.Parse(input);
            makeup.Mapping.ShouldBe(Mapping.LoRom);
            makeup.ExHiRom.ShouldBe(false);
            makeup.ExLoRom.ShouldBe(false);
            makeup.FastRom.ShouldBe(true);
            makeup.Sa1.ShouldBe(false);
        }

        public void ShouldParseHiRomFastRom()
        {
            byte input = 0x31;
            var makeup = RomMakeup.Parse(input);
            makeup.Mapping.ShouldBe(Mapping.HiRom);
            makeup.ExHiRom.ShouldBe(false);
            makeup.ExLoRom.ShouldBe(false);
            makeup.FastRom.ShouldBe(true);
            makeup.Sa1.ShouldBe(false);
        }

        public void ShouldParseSa1()
        {
            byte input = 0x23;
            var makeup = RomMakeup.Parse(input);
            makeup.Mapping.ShouldBe(Mapping.HiRom);
            makeup.ExHiRom.ShouldBe(false);
            makeup.ExLoRom.ShouldBe(false);
            makeup.FastRom.ShouldBe(false);
            makeup.Sa1.ShouldBe(true);
        }

        public void ShouldThrowExLoRomExHiRom()
        {
            byte input = 0x26;
            Should.Throw<ArgumentException>(() => RomMakeup.Parse(input));
        }

        public void ShouldThrowNo0x20Bit()
        {
            byte input = 0x00;
            Should.Throw<ArgumentException>(() => RomMakeup.Parse(input));
        }
    }
}