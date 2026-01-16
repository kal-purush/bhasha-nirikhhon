using System;

namespace BuddyDomain.ValueObjects.Battle
{
    public readonly struct HealthPoint
    {
        private readonly int value;

        public HealthPoint(int value)
        {
            if (value < 0)
            {
                throw new ArgumentException("HP value must be positive or 0.");
            }

            this.value = value;
        }

        public static HealthPoint Zero => new HealthPoint(0);

        public static bool operator ==(HealthPoint left, HealthPoint right)
            => left.Equals(right);

        public static bool operator !=(HealthPoint left, HealthPoint right)
            => !(left == right);

        public override bool Equals(object obj)
        {
            if (obj is HealthPoint health)
            {
                return Equals(health);
            }

            return false;
        }

        public override int GetHashCode() => value;

        public bool Equals(HealthPoint health)
            => this.value == health.value;

        public HealthPoint Reduce(HealthPoint down)
        {
            int newerValue = Math.Max(0, value - down.value);
            return new HealthPoint(newerValue);
        }

        public HealthPoint Recover(HealthPoint inc, MaxHealthPoint max)
        {
            var refHp = max.MaxHealth;
            int after = Math.Min(refHp.value, this.value + inc.value);
            return new HealthPoint(after);
        }

    }
}