
Console.WriteLine("238. Product of Array Except Self");
Console.WriteLine(ProductExceptSelf(new int[] { 1, 2, 3, 4 }));
Console.WriteLine(ProductExceptSelf(new int[] { -1, 1, 0, -3, 3 }));


int[] ProductExceptSelf(int[] nums)
{
    var results = new int[nums.Length];
    Array.Fill(results, 1);

    int prefix = 1;
    for (int i = 0; i < nums.Length; i++)
    {
        results[i] *= prefix;
        prefix*= nums[i];
    }

    int postfix = 1;
    for (int i = nums.Length -1; i >= 0; i--)
    {
        results[i] *= postfix;
        postfix*= nums[i];
    }


    return results;
}
