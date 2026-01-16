using System.Text;

namespace LeetCode
{
    public class LargestSum
    {
        public static bool compare(string c1,string c2)
        {
            for (int i = 0; i < Math.Min(c1.Length,c2.Length); i++)
            {
                if (c1[i] > c2[i])
                    return true;
                else if (c1[i] < c2[i])
                    return false;
            }
            if(c1.Length != c2.Length)
                return compare(c1+c2,c2+c1);
            return true;
        }
        public static string LargestNumber(int[] nums)
        {
            for (int i = 0; i < nums.Length - 1; i++)
            {
                for (int j = 0; j < nums.Length-i-1; j++)
                {
                    string c1 = (nums[j] + "");
                    string c2 = (nums[j+1] + "");
                    if (compare(c1,c2))
                    {
                        int temp = nums[j];
                        nums[j] = nums[j + 1];
                        nums[j + 1] = temp;
                    }
                }
            }
            if (nums[nums.Length - 1]==0)
                return "0";
            StringBuilder sb = new();
            for (int i = nums.Length-1; i >=0 ; i--)
                sb.Append(nums[i]);
            return sb.ToString();
        }
    }
}