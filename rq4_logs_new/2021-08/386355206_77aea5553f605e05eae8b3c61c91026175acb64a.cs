using System;
using System.Collections.Generic;
using ClassesTasks;

public class Tree : ITree
{
    public int Height { get; set; }
    public List<Branch> Branches { get; set; }
    
    public class Branch
    {  
        public int Length { get; set; }
        public int Position { get; set; }
    }
    public Tree()
    {
        Height = 1;
        Branches = new List<Branch>();
    }

    public void GrowTrunk()
    {
        Height++;
    }

    public void NewBranch()
    {
        Branches.Add(new Branch{ Length = 1, Position = Height });
        Console.WriteLine();
    }

    public void GrowBranches()
    {
        for(int i = 0; i < Branches.Count; i++)
            Branches[i].Length++;
    }

    public void Ouch(int n)
    {
        if(n > 0 && Branches.Count >= n)
            Branches.RemoveAt(n);
    }

    public string Description()
    {
        string text = String.Empty;
        int branchesCount = Branches.Count;
        if(branchesCount > 0)
        {
            string[] position = new string[branchesCount];
            string[] length = new string[branchesCount];
            for(int i = 0; i < branchesCount; i++)
            {
                position[i] = Branches[i].Position.ToString();
                length[i] = Branches[i].Length.ToString();
            }
            text = $" that have position(s): {String.Join(",", position)} and length(s): {String.Join(",", length)}";
        }

        return $"The tree trunk is {Height} unit(s) tall! There are {branchesCount} branch(es){text}!";
    }
}