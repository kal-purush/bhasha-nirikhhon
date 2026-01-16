using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic.FileIO;

namespace RobotFactory
{
    public class RobotFactory
    {
        private static RobotFactory _instance;

        private static Random _random;
        public HashSet<string> CreatedRobotsNames { get; } = new HashSet<string>();

        private readonly string _availableLetters =
            string.Concat(Enumerable.Range('A', 'Z' - 'A' + 1).Select(n => (char) n));

        private readonly string _availableNumbers =
            string.Concat(Enumerable.Range('0', '9' - '0' + 1).Select(n => (char) n));

        private RobotFactory() { }

        public static RobotFactory Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new RobotFactory();
                    _random = new Random();
                }

                return _instance;
            }
        }

        private string GenerateNewName(string pattern)
        {
            var builder = new StringBuilder();
            while (!Regex.IsMatch(builder.ToString(), pattern) || CreatedRobotsNames.Contains(builder.ToString()))
            {
                builder.Clear();
                builder.Append(string.Concat(Enumerable.Repeat(_availableLetters, 2)
                    .Select(s => s[_random.Next(s.Length)])));
                builder.Append(string.Concat(Enumerable.Repeat(_availableNumbers, 3)
                    .Select(s => s[_random.Next(s.Length)])));
            }

            var name = builder.ToString();
            CreatedRobotsNames.Add(name);
            return name;
        }

        public void ResetRobot(Robot robot)
        {
            robot.Name = GenerateNewName(robot.NamePattern);
        }

        public Robot CreateRobot()
        {
            if (Enum.TryParse(typeof(RobotType), Enum.GetNames(typeof(RobotType)).OrderBy(o => _random.Next()).FirstOrDefault(), true, out var robotType))
            {
                var robot = new Robot((RobotType) robotType);
                string robotName = GenerateNewName(robot.NamePattern);
                robot.Name = robotName;
                return robot;
            }
            throw new ArgumentException("RobotType is invalid!", nameof(robotType));

        }

    }
}