using System;
using System.Text.RegularExpressions;

namespace RobotFactory
{
    public class Robot
    {
        private string _name;
        public string Name
        {
            get => _name;
            set
            {
                if (Regex.IsMatch(value, NamePattern))
                {
                    _name = value;
                }
                else
                {
                    throw new ArgumentException($"New name doesn't match required pattern! of {NamePattern}", nameof(value));
                }
            }

        }
        public string NamePattern { get; }
        public RobotType Type { get; }

        public Robot(RobotType type)
        {
            NamePattern = @"[A-Z]{2}[0-9]{3}";
            Type = type;
        }

        public override string ToString()
        {
            return $"Robot named {Name} of type {Type}";
        }

    }
}