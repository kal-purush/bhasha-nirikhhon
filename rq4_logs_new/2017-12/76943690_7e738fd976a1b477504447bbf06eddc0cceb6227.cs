using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Advent2017.Solutions
{
    class Day7
    {
        public string FindBottomProgram(string[] sample)
        {
            BuildNetwork(sample);
            return  _programs.Values.Single(x => x.Parent == null).Name;
        }

        private void BuildNetwork(string[] sample)
        {
            Regex matcher = new Regex(@"(\w+) \((\d+)\)(?: -> )?(.*)");
            foreach (var s in sample)
            {
                var match = matcher.Match(s);
                var prog = new Program
                {
                    Name = match.Groups[1].Value,
                    Size = Convert.ToInt32(match.Groups[2].Value),
                    ChildrenInput = match.Groups[3].Value,
                    Children = new List<Program>()
                };
                _programs.Add(prog.Name, prog);
            }

            foreach (var kvp in _programs)
            {
                if (string.IsNullOrEmpty(kvp.Value.ChildrenInput)) continue;
                var children = kvp.Value.ChildrenInput.Split(',');
                foreach (var child in children)
                {
                    var parentNode = kvp.Value;
                    var childNode = _programs[child.Trim()];
                    childNode.Parent = kvp.Value;
                    parentNode.Children.Add(childNode);
                }
            }
        }

        private Dictionary<string, Program> _programs = new Dictionary<string, Program>();

        public int FindCorrectedWeight(string[] input)
        {
            BuildNetwork(input);
            
            var node = _programs.Values.Single(x => x.Parent == null);

            return AdjustNodeToWeight(node, 0);
        }

        public int AdjustNodeToWeight(Program node, int targetWeight)
        {
            Console.WriteLine("Adjust node to weight for " + node.Name);
            if (node.IsBalanced)
            {
                var delta = targetWeight - node.TotalSize;
                return node.Size + delta;
            }
            //find the child node that is different to the others and recurse on that node.
            var group = node.Children.GroupBy(x => x.TotalSize).OrderByDescending(x => x.Count());
            var problemChild = group.Last().First();
            var targetSize = group.First().Key;
            return AdjustNodeToWeight(problemChild, targetSize);
        }
    }

    class Program
    {
        public string Name { get; set; }
        public int Size { get; set; }
        public Program Parent { get; set; }
        public List<Program> Children { get; set; }
        public string ChildrenInput { get; set; }

        public bool IsBalanced
        {
            get { return Children.All(x => x.TotalSize == Children[0].TotalSize); }
        }

        public int TotalSize
        {
            get { return Children.Sum(x => x.TotalSize) + Size; }
        }
    }
}