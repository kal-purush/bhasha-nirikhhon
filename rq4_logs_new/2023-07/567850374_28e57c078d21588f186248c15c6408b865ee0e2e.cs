namespace Entities
{
    internal class Registry
    {
        public string User { get; set; }
        public DateTime Entry { get; set; }

        public Registry(string user, DateTime entry)
        {
            User = user;
            Entry = entry;
        }

        public Registry(Registry registry)
        {
            User = registry.User;
            Entry = registry.Entry;
        }

        public override bool Equals(object? obj)
        {
            if (obj == null || !(obj is Registry))
            {
                return false;
            }

            Registry other = obj as Registry;
            return User.Equals(other.User);
        }
        public override int GetHashCode()
        {
            return User.GetHashCode();
        }

        public override string ToString()
        {
            return "Username: " + User + "\n" +
                   "Entry: " + Entry;
        }
    }
}