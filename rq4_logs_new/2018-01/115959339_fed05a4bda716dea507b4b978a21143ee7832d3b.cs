namespace Entities
{
    public abstract class ListenerProtocol
    {
        protected abstract string Greetings { get; }
        protected abstract string Goodbye { get; }
        protected abstract string GetNext(string req);
        public abstract string Accept { get; }
        public abstract string EOF { get; }
        public abstract string Exit { get; }
        public string Next(string s)
        {
            var request = s.TrimEnd(EOF.ToCharArray());

            if (request.Equals(Accept))
                return Greetings;

            if (request.Equals(Exit))
                return Goodbye;

            return GetNext(request);
        }
    }
}