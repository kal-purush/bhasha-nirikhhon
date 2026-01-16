using System;
using System.Collections.Generic;
using System.Text;

public class BLEU
{
    public static int str1_length;
    public static int str2_length;
    public class Ngram
    {
        private int N;
        private string OriginalSentence;
        public Ngram(string sentence, int n)
        {
            N = n;
            OriginalSentence = sentence;
        }

        public IEnumerable<string> ngram_queue(int N)
        {
            if (OriginalSentence == null)
            {
                Console.WriteLine("The test sentence is empty");
            }
            else
            {
                string sentence = OriginalSentence + " ";
                StringBuilder completed = new StringBuilder();
                //save the result
                Queue<int> Word_len = new Queue<int>();
                //save the length of each word
                int count = 0;
                //count the word have been catched
                int lastWord = 0;
                //the length of lastword in queue

                for (int i = 0; i < sentence.Length; i++)
                {
                    if (sentence[i] != ' ')
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
        }

        public Dictionary<string, int> NgramToDict(int n)
        //save the ngram to dictionary
        {
            Dictionary<string, int> ngram = new Dictionary<string, int>();
            if (n == 1)
            {
                var SentencePiece = OriginalSentence.Split(" ");
                for (int i = 0; i < SentencePiece.Length; i++)
                {
                    if (!ngram.ContainsKey(SentencePiece[i]))
                    {
                        ngram[SentencePiece[i]] = 1;
                    }
                    else
                    {
                        ngram[SentencePiece[i]] += 1;
                    }
                }
                return ngram;
            }
            else
            {
                IEnumerable<string> result = ngram_queue(n);
                foreach (string temp in result)
                {
                    if (!ngram.ContainsKey(temp))
                    {
                        ngram[temp] = 1;
                    }
                    else
                    {
                        ngram[temp] += 1;
                    }
                }
                return ngram;
            }
        }
    }

    public static void bleu(Ngram N1, Ngram N2)
    {
        List<Dictionary<string, int>> N1gram = new List<Dictionary<string, int>>();
        List<Dictionary<string, int>> N2gram = new List<Dictionary<string, int>>();
        for (int i = 1; i <= 4; i++)
        {
            N1gram.Add(N1.NgramToDict(i));
            N2gram.Add(N2.NgramToDict(i));
        }
        Dictionary<string, double> result = compareTwoGram(N1gram, N2gram);
        foreach (string i in result.Keys)
        {
            Console.WriteLine("p_{0} is {1}", i, result[i]);
        }
    }
    public static Dictionary<string, double> compareTwoGram(List<Dictionary<string, int>> Dict_list1, List<Dictionary<string, int>> Dict_list2)
    {
        Dictionary<string, double> result = new Dictionary<string, double>();
        double BP = 0.00;
        double logPn = 0.00;
        //penalty
        for (int m = 0; m <= 3; m++)
        {
            //1,2,3,4gram
            int count_clip = 0;
            //save the number of correct prediction
            int count = 0;
            //save the number of all grams
            int temp_length = 0;
            double temp = 0.0000;
            //save the P_i possibility
            Dictionary<string, int> Dict1 = Dict_list1[m];
            Dictionary<string, int> Dict2 = Dict_list2[m];
            foreach (string key in Dict1.Keys)
            {
                //Console.WriteLine(key);
                if (Dict2.ContainsKey(key))
                {
                    count_clip += Math.Min(Dict1[key], Dict2[key]);
                    //modified precision
                }
            }
            foreach (int i in Dict1.Values)
            {
                count += i;
            }
            foreach (int j in Dict2.Values)
            {
                temp_length += j;
            }
            if (m == 0)
            //1-gram elements number equal to string length
            {
                str1_length = count;
                str2_length = temp_length;
            }
            //Console.WriteLine("{0},{1}",count_clip, count);
            temp = count_clip / (double)count;
            if (temp > 0)
            //avoid log 0;
            {
                logPn += Math.Log(temp);
                //Console.WriteLine(logPn);
            }
            result[(m + 1).ToString()] = temp;
        }
        if (str1_length > str2_length)
        // calculate penalty
        {
            BP = 1;
        }
        else
        {
            BP = Math.Exp(1 - str2_length / (double)str1_length);
        }
        logPn /= 4.00;
        //totally 4-gram
        //give each gram with the same weight
        result["bleu"] = BP * Math.Exp(logPn);
        return result;
    }
    class Program
    {
        static void Main(string[] args)
        {
            string str1 = "Going to play basketball this afternoon ?";
            string str2 = "Going to play basketball in the afternoon ?";
            Ngram test = new Ngram(str1, 4);
            Ngram test1 = new Ngram(str2, 4);
            bleu(test, test1);
        }
    }
}