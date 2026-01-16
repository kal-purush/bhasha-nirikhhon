namespace SpringChallenge2022.Models
{
    public class RankedMonster
    {
        public int ThreatLevel { get; }

        public int TurnsToReach { get; }

        public int ShotsNeeded { get; }

        public Monster Monster { get; }

        public RankedMonster(
            Monster monster,
            int threatLevel,
            int turnsToReach,
            int shotsNeeded)
        {
            Monster = monster;
            ThreatLevel = threatLevel;
            TurnsToReach = turnsToReach;
            ShotsNeeded = shotsNeeded;
        }

        public override string ToString()
            => $"Id: {Monster.Id} : ThreatLevel {ThreatLevel} : TurnsToReach {TurnsToReach} : ShotsNeeded {ShotsNeeded}";
    }
}