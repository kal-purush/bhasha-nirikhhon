namespace Smelt.Data
{
    public class RomHeader
    {
        public string Title { get; set; }
        public RomMakeup Makeup { get; set; }
        public int Type { get; set; }
        public int RomSize { get; set; }
        public int SramSize { get; set; }
        public int CreatorLicenseIdCode { get; set; }
        public int VersionNum { get; set; }
        public int ChecksumComplement { get; set; }
        public int Checksum { get; set; }


        public RomHeader()
        {
            Makeup = new RomMakeup();
        }
    }
}