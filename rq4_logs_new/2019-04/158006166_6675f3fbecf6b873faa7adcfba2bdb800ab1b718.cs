using System.Diagnostics;

namespace LeedCodeBusiness.Algos.easy
{
    public class Sum
    {
        public int[] TwoSum(int[] nums, int target)
        {
            int[] _sum = new int[2];

            for (int firstNumber = 0; firstNumber < nums.Length; firstNumber++)
            {                
                
                for (int nextNumber = 0; nextNumber < nums.Length; nextNumber++)
                {
                    if (target == nums[firstNumber] + nums[nextNumber])
                    {
                        _sum[1] = firstNumber;
                        _sum[0] = nextNumber;
                        break;                        
                    }
                }
            }

            return _sum;
        }
    }
}