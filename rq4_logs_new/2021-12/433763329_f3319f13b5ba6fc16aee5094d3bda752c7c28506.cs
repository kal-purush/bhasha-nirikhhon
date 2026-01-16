using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdventOfCode2021
{
    internal class Day10
    {

        public static long Run()
        {

            List<string> lines = File.ReadAllLines("Data/Day10Input.txt").ToList();

            List<string> incompleteLines =  new List<string>();
            foreach (var line in lines)
            {
                if (lineScore(line) == 0)
                {
                    incompleteLines.Add(line);
                }
            }
            List<long> sumList = new List<long>();
            foreach (var line in incompleteLines)
            {
                List<char> chunkStack = new List<char>();
                foreach (var character in line)
                {
                    if (character == '(' || character == '[' || character == '{' || character == '<')
                    {
                        chunkStack.Add(character);
                    }
                    else if (character == ')' && chunkStack.Last() == '(' ||
                             character == ']' && chunkStack.Last() == '[' ||
                             character == '}' && chunkStack.Last() == '{' ||
                             character == '>' && chunkStack.Last() == '<')
                    {
                        chunkStack.RemoveAt(chunkStack.Count - 1);
                    }
                }

                long sum = 0;
                chunkStack.Reverse();

                foreach (var item in chunkStack)
                {
                    sum *= 5;
                    if (item == '(')
                    {
                        sum += 1;
                    }
                    if (item == '[')
                    {
                        sum += 2;
                    }
                    if (item == '{')
                    {
                        sum += 3;
                    }
                    if (item == '<')
                    {
                        sum += 4;
                    }
                }
                sumList.Add(sum);
            }
            sumList.Sort();
            return sumList[(sumList.Count - 1)/2];
        }

        private static int lineScore(string line)
        {
            List<char> chunkStack = new List<char>();

            foreach (var character in line)
            {
                if (character == '(' || character == '[' || character == '{' || character == '<')
                {
                    chunkStack.Add(character);
                }
                else if (character == ')' && chunkStack.Last() == '(' ||
                         character == ']' && chunkStack.Last() == '[' ||
                         character == '}' && chunkStack.Last() == '{' ||
                         character == '>' && chunkStack.Last() == '<')
                {
                    chunkStack.RemoveAt(chunkStack.Count - 1);
                }
                else if (character == ')')
                {
                    return 3;
                }
                else if (character == ']')
                {
                    return 57;
                }
                else if (character == '}')
                {
                    return 1197;
                }
                else if (character == '>')
                {
                    return 25137;
                }
            }
            return 0;
        }
    }
}