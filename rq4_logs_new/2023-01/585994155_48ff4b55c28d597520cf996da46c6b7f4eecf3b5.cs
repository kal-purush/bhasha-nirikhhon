namespace PDFBattleBoard.Model
{

    enum PcClass
    {
        Warrior,
        Wizard,
        Scout,
        Priest
    }

    enum PcRace
    {
        Human,
        HalfOrc,
        Elf
    }

    internal class CharacterDetails
    {
        public string Name { get; set; }
        public PcClass PlayerClass { get; set; }
        public PcRace CharacterRace { get; set; }
        public int Points { get; set; }
        public int ResChances { get; set; }

        public Armour CharacterArmour { get; set; }
    }
}