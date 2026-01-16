using System;

namespace BuddyDomain.Battle.ValueObjects.Skill
{
    public readonly struct Priority : IComparable<Priority>, IEquatable<Priority>
    {
        private readonly int priority;

        public Priority(int priority) => this.priority = priority;

        public static bool operator <(Priority a, Priority b) => a.CompareTo(b) < 0;

        public static bool operator >(Priority a, Priority b) => a.CompareTo(b) > 0;

        public static bool operator ==(Priority a, Priority b) => a.Equals(b);

        public static bool operator !=(Priority a, Priority b) => !(a == b);

        public int CompareTo(Priority other) => priority.CompareTo(other.priority);

        public bool Equals(Priority other) => priority == other.priority;

        public override bool Equals(object obj) => obj is Priority other && Equals(other);

        public override int GetHashCode() => priority;
    }
}