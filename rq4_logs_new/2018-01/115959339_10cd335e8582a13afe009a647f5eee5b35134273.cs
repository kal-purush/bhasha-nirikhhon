using System;
using System.Linq;
using Administration.UseCases;
using Entities;

namespace Administration.Adapters
{
    internal class CommandParser
    {
        private readonly FeatureCommandCollection _coll;
        public Port<string> InputPort { get; }
        public Port<ICommand> OutputPort { get; }

        public CommandParser(FeatureCommandCollection coll, Port<string> input,
            Port<ICommand> output)
        {
            _coll = coll;
            InputPort = input;
            OutputPort = output;

            InputPort.OnDataReceived += (sender, s) => Parse(s);
        }

        private void Parse(string s)
        {
            var group = string.Empty;
            var commandName = string.Empty;
            var tokens = s.Split(' ');
            if (tokens.Length == 2)
            {
                group = tokens[0];
                commandName = tokens[1];
            }

            ICommand cmd = new NotFoundCommand();
            if (_coll.ContainsKey(group))
            {
                cmd = _coll[group]
                          .FirstOrDefault(c =>
                              string.Compare(c.Name,
                                  commandName,
                                  StringComparison.InvariantCultureIgnoreCase) == 0)
                      ?? new NotFoundCommand();
            }

            OutputPort.Transfer(cmd);
        }
    }
}