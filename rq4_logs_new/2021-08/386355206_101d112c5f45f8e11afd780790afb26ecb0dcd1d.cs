using System;


namespace ArraysKatas
{
    public class WellOfIdeas_EV
    {
        /*For every good kata idea there seem to be quite a few bad ones!In this kata you need to check the provided
          array (x) for good ideas 'good' and bad ideas 'bad'. If there are one or two good ideas, return 'Publish!',
          if there are more than 2 return 'I smell a series!'. If there are no good ideas, as is often the case, 
          return 'Fail!'.*/
        public static string Well(string[] x)
        {
            // var findString = Array.Find(x, element => element == "good");
            // if (findString > 2) return "I smell a series!";
            // if (findString > 0) return "Publish!";
            // return "Fail!";
            
            int goodString = 0;
            foreach (string idea in x)
            {
                if (idea.ToLower() == "good")
                    goodString++;
            }
            if (goodString == 0) {return "Fail!";}
            if (goodString <= 2) {return "Publish!";}
            return "I smell a series!";
            
            // int count = x.Count(v => v == "good");
            // return (count > 2) ? "I smell a series!" : (count >= 1) ? "Publish!" : "Fail!";

        }
    }
}