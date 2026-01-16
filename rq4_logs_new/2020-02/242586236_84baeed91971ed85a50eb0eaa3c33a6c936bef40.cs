using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;

namespace Assignment2DIS
{
    class Program
    {
        public static void DisplayArray(int[] a)
        {
            foreach (int n in a)
            {
                Console.Write(n + " ");
            }
        }
        public static int[] TargetRange(int[] l1, int t)
        {
            try
            {
                int n = l1.Length;
                int a = -1, b = -1;

                for (int i = 0; i < n; i++)
                {
                    if (t != l1[i])
                        continue;
                    if (a == -1)
                        a = i;
                    b = i;
                }
                if (a != -1)
                {
                    Console.WriteLine("[" + a + "," + b + "]");

                }
                else
                    Console.Write("[-1,-1]");
            }
            catch (Exception)
            {
                throw;
            }
            return new int[] { };
        }

        public static string StringReverse(string s)
        {
            try
            {
                int strLeng = s.Length - 1;
                String reverse = "", temp = "";

                for (int i = 0; i <= strLeng; i++)
                {
                    temp += s[i];
                    if ((s[i] == ' ') || (i == strLeng))
                    {
                        for (int j = temp.Length - 1; j >= 0; j--)
                        {
                            reverse += temp[j];
                            if ((j == 0) && (i != strLeng))
                                reverse += " ";
                        }
                        temp = "";
                    }
                }
                return reverse;
            }
            catch (Exception)
            {
                throw;
            }
            //return null;
        }

        public static int MinimumSum(int[] l2)
        {
            try
            {
                int sum = l2[0], prev = l2[0];
                int n = l2.Length;

                for (int i = 1; i < n; i++)
                {

                    if (l2[i] <= prev)
                    {
                        prev = prev + 1;
                        sum = sum + prev;
                    }

                    // No violation.

                    else
                    {
                        sum = sum + l2[i];
                        prev = l2[i];
                    }
                }

                return sum;
            }
            catch (Exception)
            {
                throw;
            }
            //return 0;
        }

        public static string FreqSort(string s2)
        {
            try
            {
                {
                    if (string.IsNullOrEmpty(s2))
                        return s2;

                    Dictionary<char, int> cache = new Dictionary<char, int>();
                    Dictionary<int, List<char>> rcache = new Dictionary<int, List<char>>();

                    // calculate frequencies by char
                    foreach (var c in s2)
                    {
                        if (!cache.ContainsKey(c))
                        {
                            cache.Add(c, 0);
                        }

                        cache[c]++;
                    }

                    // reverse cache: chars by frequency
                    foreach (var k in cache.Keys)
                    {
                        if (!rcache.ContainsKey(cache[k]))
                            rcache.Add(cache[k], new List<char>());

                        rcache[cache[k]].Add(k);
                    }

                    // get result/
                    System.Text.StringBuilder sb = new StringBuilder();
                    var l = rcache.Keys.ToList();
                    l.Sort();
                    l.Reverse();

                    foreach (var f in l)
                    {
                        foreach (var c in rcache[f])
                            for (int i = 0; i < f; i++)
                                sb.Append(c);
                    }

                    return sb.ToString();
                }

            }
            catch (Exception)
            {
                throw;
            }
            //return null;
        }

        public static int[] Intersect1(int[] nums1, int[] nums2)
        {
            try
            {
                List<int> ls = new List<int>();

                for (int i = 0; i < nums1.Length; i++)
                {
                    ls.Add(nums1[i]);
                }

                for (int i = 0; i < nums2.Length; i++)
                {
                    if (ls.Contains(nums2[i]))

                        Console.Write(nums2[i] + " ");
                }
            }
            catch
            {
                throw;
            }
            return new int[] { };
        }

        public static int[] Intersect2(int[] nums1, int[] nums2)
        {
            try
            {
                Array.Sort(nums1);
                Array.Sort(nums2);
                int i = 0, j = 0;

                while (i < nums1.Length && j < nums2.Length)
                {
                    if (nums1[i] < nums2[j])
                    {
                        i++;
                    }
                    else if (nums2[j] < nums1[i])
                    {
                        j++;
                    }
                    else
                    {
                        Console.Write(nums2[j++] + " ");
                        i++;
                    }
                }
            }
            catch
            {
                throw;
            }
            return new int[] { };
        }

        public static bool ContainsDuplicate(char[] arr, int k)
        {
            try
            {
                Dictionary<char, int> dict = new Dictionary<char, int>();
                for (int i = 0; i < arr.Length; i++)
                {
                    if (dict.ContainsKey(arr[i]))
                    {
                        if (i - dict[arr[i]] <= k)
                        {
                            return true;
                        }
                    }
                    else
                        dict[arr[i]] = i;
                }

                return false;
            }
            catch (Exception)
            {
                throw;
            }
            //return default;
        }

        public static int GoldRod(int rodLength)
        {
            try
            {
                if (rodLength == 2)
                    return 1;
                else if (rodLength == 3)
                    return 2;
                else if (rodLength == 4)
                    return 4;
                else if (rodLength == 5)
                    return 6;
                else if (rodLength == 6)
                    return 9;
                else
                    return 3 * GoldRod(rodLength - 3);

            }
            catch (Exception)
            {
                throw;
            }
            //return 0;
        }

        public static bool DictSearch(string[] userDict, string keyword)
        {
            try
            {
                int mismatch = 0;
                for (int j = 0; j < userDict.Length; j++)
                {
                    if (userDict[j].Length == keyword.Length)
                    {
                        for (int k = 0; k < userDict[j].Length; k++)
                        {
                            if (userDict[j][k] != keyword[k])
                            {
                                mismatch++;
                            }

                        }

                        if (mismatch == 1)
                        {
                            return true;
                        }
                        mismatch = 0;
                    }
                }

                return false;
            }
            catch (Exception)
            {
                throw;
            }
            
        }

        public static void SolvePuzzle()
        {
            try
            {
                //Write Your Code Here
            }
            catch (Exception)
            {
                throw;
            }
        }
        static void Main(string[] args)
        {
            Console.WriteLine("Question 1");
            int[] l1 = new int[] { 5, 6, 6, 9, 9, 12 };
            int target = 9;
            int[] r = TargetRange(l1, target);
            // Write your code to print range r here

            Console.WriteLine();
            Console.WriteLine("Question 2");
            string s = "University of South Florida";
            string rs = StringReverse(s);
            Console.WriteLine(rs);

            Console.WriteLine();
            Console.WriteLine("Question 3");
            int[] l2 = new int[] { 2, 2, 3, 5, 6 };
            int sum = MinimumSum(l2);
            Console.WriteLine(sum);

            Console.WriteLine();
            Console.WriteLine("Question 4");
            string s2 = "Dell";
            string sortedString = FreqSort(s2);
            Console.WriteLine(sortedString);

            Console.WriteLine();
            Console.WriteLine("Question 5-Part 1");
            int[] nums1 = { 1, 2, 2, 1 };
            int[] nums2 = { 2, 2 };
            Console.WriteLine("Part 1- Intersection of two arrays is: ");
            int[] intersect1 = Intersect1(nums1, nums2);
            DisplayArray(intersect1);
            Console.WriteLine("\n");
            Console.WriteLine("Question 5-Part 2");
            Console.WriteLine("Part 2- Intersection of two arrays is: ");
            int[] intersect2 = Intersect2(nums1, nums2);
            DisplayArray(intersect2);
            Console.WriteLine("\n");

            Console.WriteLine("Question 6");
            char[] arr = new char[] { 'a', 'g', 'h', 'a' };
            int k = 3;
            Console.WriteLine(ContainsDuplicate(arr, k));

            Console.WriteLine();
            Console.WriteLine("Question 7");
            int rodLength = 8;
            int priceProduct = GoldRod(rodLength);
            Console.WriteLine(priceProduct);

            Console.WriteLine();
            Console.WriteLine("Question 8");
            string[] userDict = new string[] { "rocky", "usf", "hello", "apple" };
            string keyword = "hhllo";
            Console.WriteLine(DictSearch(userDict, keyword));

            Console.WriteLine();
            Console.WriteLine("Question 8");
            SolvePuzzle();
        }
    }
}