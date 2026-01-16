namespace DBT.Skills.BlackPowerBall
{
    public sealed class BlackPowerBallDefinition : SkillDefinition
    {
        public BlackPowerBallDefinition() : base("BlackPowerBall", "Black Power Ball", "A powerful blast attack that can also be rapidly fired as a barrage.", new BlackPowerBallCharacteristics())
        {
        }
    }

    public sealed class BlackPowerBallCharacteristics : SkillCharacteristics
    {
        public BlackPowerBallCharacteristics() : base(new BlackPowerBallChargeCharacteristics(), 130, 0.65f, 10f, 5f, 0.65f, 0.05f, 1.02f, 2f, 1f)
        {
        }
    }

    public sealed class BlackPowerBallChargeCharacteristics : SkillChargeCharacteristics
    {
        public BlackPowerBallChargeCharacteristics() : base(70, 3, 100, 45)
        {
        }
    }
}