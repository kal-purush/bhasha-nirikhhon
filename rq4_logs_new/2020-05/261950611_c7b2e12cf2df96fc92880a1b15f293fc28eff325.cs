using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

public class Ngram
{
    public void ngram_n2(string sentence, int N)
    {
        string[] Sentence = sentence.Split(" ");
        //split to single word
        List<string> completed = new List<string>();
        //save the result
        if (sentence == null)
        {
            Console.WriteLine("The test sentence is empty");
        }
        else
        {
            for (int i = 0; i <= (Sentence.Length - N); i++)
            //loop word list
            {
                List<string> temp = new List<string>();
                for (int j = i; j < i + N; j++)
                //complete one n-gram each loop
                {
                    temp.Add(Sentence[j]);
                }
                completed.Add(string.Join(" ", temp));
            }
            for (int i = 0; i < completed.Count; i++)
            {
                Console.WriteLine(completed[i]);
            }
        }
    }
    public IEnumerable<string> ngram_queue(string sentence, int N)
    {
        sentence = sentence + " ";
        StringBuilder completed = new StringBuilder();
        //save the result
        Queue<int> Word_len = new Queue<int>();
        //save the length of each word
        int count = 0;
        //count the word have been catched
        int lastWord = 0;
        //the length of lastword in queue
        if (sentence == null)
        {
            Console.WriteLine("The test sentence is empty");
        }
        for (int i = 0; i < sentence.Length; i++)
        {
            if (char.IsLetterOrDigit(sentence[i]) || sentence[i] == '\'')
            //make sure the sentence[i] is letter digit or '
            {
                completed.Append(sentence[i]);
                lastWord++;
            }
            else
            //when suffer the " "(a word completed)
            {
                if (lastWord > 0)
                //if there are letters in buffer
                {
                    Word_len.Enqueue(lastWord);
                    lastWord = 0;
                    count++;
                    if (count == N)
                    //if the counted words equal to requied N
                    {
                        yield return completed.ToString();
                        completed.Remove(0, Word_len.Dequeue() + 1);
                        count -= 1;
                    }
                    completed.Append(" ");
                }
            }
        }
    }

    public void readNgram(string sentence, int N)
    {
        Console.WriteLine("1-gram");
        foreach (int i in Enumerable.Range(0, sentence.Split(" ").Count()))
        {
            Console.WriteLine(sentence.Split(" ")[i]);
        }
        if (N >= 2)
        {
            for (int i = 2; i <= N; i++)
            {
                Console.WriteLine("{0}-gram", i);
                IEnumerable<string> result = ngram_queue(sentence, i);
                foreach (string temp in result)
                {
                    Console.WriteLine(temp);
                }
            }
        }
    }
}
public class Ngram_test
{
    public static void Main(string[] arg)
    {
        Ngram test = new Ngram();
        //test.ngram_n2("Hello world my name is xd", 3);
        test.readNgram("Hello world my name is xd", 3);
    }
}