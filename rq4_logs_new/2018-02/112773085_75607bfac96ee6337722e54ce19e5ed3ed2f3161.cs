using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AdventOfCode
{
    public class Day25
    {
        public static int Task1(string[] blueprint)
        {
            var blueprintParsed = ParseBlueprint(blueprint);
            var turningMachine = new TurningMachine(blueprintParsed.States, blueprintParsed.StartingState);

            for (int i = 0; i < blueprintParsed.NumberOfExecutions; i++)
            {
                turningMachine.Execute();
            }

            return turningMachine.Checksum();
        }

        public static int Task2(string[] components)
        {
            return 0;
        }    
        
        private static Blueprint ParseBlueprint(string[] blueprint)
        {
            var startingState = SecondLastChar(blueprint[0]);

            var numberOfExecutionsLine = blueprint[1];
            var numberOfExecutions = numberOfExecutionsLine
                .Split(' ')
                .Skip(5)
                .Take(1)
                .Select(int.Parse)
                .Single();

            var states = new Dictionary<char, State>();

            var stateLine = 3;
            for (var i = 0; i < (blueprint.Length - 2) / 10; i++)
            {
                var stateName = SecondLastChar(blueprint[stateLine]);
                var stateLines = blueprint
                    .Skip(stateLine + 1)
                    .Take(8)
                    .ToArray();
                var state = ParseState(stateLines);
                stateLine += 10;
                states.Add(stateName, state);
            }

            return new Blueprint(states, startingState, numberOfExecutions);
        }

        private static State ParseState(string[] state)
        {
            var kalle = state
                .Skip(1)
                .Take(3)
                .ToArray();

            var anka = state
                .Skip(5)
                .Take(3)
                .ToArray();

            var zeroRule = ParseRule(kalle);
            var oneRule = ParseRule(anka);

            return new State(zeroRule, oneRule);
        }

        private static Rule ParseRule(string[] rule)
        {
            var writeValue = ParseWriteValue(rule[0]);
            var direction = ParseDirection(rule[1]);
            var nextState = SecondLastChar(rule[2]);

            return new Rule(writeValue, nextState, direction);
        }

        private static int ParseWriteValue(string writeValueLine)
        {
            return (int)char.GetNumericValue(SecondLastChar(writeValueLine));
        }

        private static Direction ParseDirection(string directionLine)
        {
            var direction = directionLine
               .Split(' ')
               .Last();

            return direction == "left." ? Direction.Left : Direction.Right;
        }

        private static char SecondLastChar(string line)
        {
            return line[line.Length - 2];
        }

        private class Blueprint
        {
            public Dictionary<char, State> States { get; }
            public char StartingState { get; }
            public int NumberOfExecutions { get; }
            public Blueprint(Dictionary<char, State> states, char startingState, int numberOfExecutions)
            {
                States = states;
                StartingState = startingState;
                NumberOfExecutions = numberOfExecutions;
            }
        }

        private class TurningMachine
        {
            private Dictionary<int, int> tape = new Dictionary<int, int>();
            private int cursor = 0;
            private Dictionary<char, State> states;
            private char nextState;
            public TurningMachine(Dictionary<char, State> states, char firstState)
            {
                this.states = states;
                nextState = firstState;
            }

            public void Execute()
            {
                var state = states[nextState];
                var value = Value();
                if (value == 0)
                {
                    ExecuteRule(state.ZeroRule);
                }
                else
                {
                    ExecuteRule(state.OneRule);
                }
            }

            public int Checksum()
            {
                return tape
                    .Values
                    .Sum();
            }

            private void ExecuteRule(Rule rule)
            {
                tape[cursor] = rule.WriteValue;
                cursor = rule.Direction == Direction.Right ? cursor + 1 : cursor - 1;
                nextState = rule.NextState;
            }

            private int Value()
            {
                if(tape.TryGetValue(cursor, out var value))
                {
                    return value;
                }

                return tape[cursor] = 0;
            }
        }

        private class State
        {
            public Rule ZeroRule { get; }
            public Rule OneRule { get; }

            public State(Rule zeroRule, Rule oneRule)
            {
                ZeroRule = zeroRule;
                OneRule = oneRule;
            }
        }

        private class Rule
        {
            public int WriteValue { get; }
            public char NextState { get; }
            public Direction Direction { get; }
            public Rule(int writeValue, char nextState, Direction direction)
            {
                WriteValue = writeValue;
                NextState = nextState;
                Direction = direction;
            }
        }

        private enum Direction
        {
            Left = 0,
            Right = 1
        }
    }
}