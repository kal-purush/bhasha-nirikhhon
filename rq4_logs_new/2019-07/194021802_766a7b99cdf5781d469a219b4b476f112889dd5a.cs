namespace DBT.Skills.CelestialRapture
{
    //Needs balancing
    public sealed class CelestialRaptureDefinition : SkillDefinition
    {
        public CelestialRaptureDefinition() : base("CelestialRapture", "Celestial Rapture", "Fires seeking ki blasts from all angles.", new CelestialRaptureCharacteristics())
        {
        }

        public sealed class CelestialRaptureCharacteristics : SkillCharacteristics
        {
            public CelestialRaptureCharacteristics() : base(new CelestialRaptureChargeCharacteristics(), 70, 1f, 16f, 5f, 1f, 0.05f, 1f, 2f, 1f)
            {
            }
        }

        public sealed class CelestialRaptureChargeCharacteristics : SkillChargeCharacteristics
        {
            public CelestialRaptureChargeCharacteristics() : base(0, 0, 499, 0)
            {
            }
        }
    }
}