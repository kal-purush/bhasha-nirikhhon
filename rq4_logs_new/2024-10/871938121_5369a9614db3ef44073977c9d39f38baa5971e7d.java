import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class L2022211978_13_Test {

    /*

    �ȼ��໮��ԭ��

    1. С�ڵ���3����
    ʾ�� nums = [2,0,1]
    ʾ�� nums = [2,0]

    2. ����3����
    ʾ�� nums = [2,0,1,1,2,2,1,0]
    ʾ�� nums = [2,0,1,2,2,1,1,0,0]

     */


    /*
    ���룺nums = [2,0,1]
    Ԥ�ڽ���� nums = [0,1,2]
     */
    @Test
    public void testSortColorsMethod(){
        Solution solution = new Solution();

        int[] nums = new int[]{2,0,1};

        solution.sortColors(nums);

        int[] exceptResult = new int[]{0,1,2};

        assertArrayEquals(exceptResult,nums);

    }

    /*
    ���룺nums = [2,0]
    Ԥ�ڽ���� nums = [0,2]
     */
    @Test
    public void testSortColorsMethod01(){
        Solution solution = new Solution();

        int[] nums = new int[]{2,0};

        solution.sortColors(nums);

        int[] exceptResult = new int[]{0,2};

        assertArrayEquals(exceptResult,nums);

    }

    /*
    ���룺nums = [2,0,1,1,2,2,1,0]
    Ԥ�ڽ���� nums = [0,0,1,1,1,2,2,2]
     */
    @Test
    public void testSortColorsMethod02(){
        Solution solution = new Solution();

        int[] nums = new int[]{2,0,1,1,2,2,1,0};

        solution.sortColors(nums);

        int[] exceptResult = new int[]{0,0,1,1,1,2,2,2};

        assertArrayEquals(exceptResult,nums);

    }

    /*
    ���룺nums = [2,0,1,2,2,1,1,0,0]
    Ԥ�ڽ���� nums = [0,0,0,1,1,1,2,2,2]
     */
    @Test
    public void testSortColorsMethod03(){
        Solution solution = new Solution();

        int[] nums = new int[]{2,0,1,2,2,1,1,0,0};

        solution.sortColors(nums);

        int[] exceptResult = new int[]{0,0,0,1,1,1,2,2,2};

        assertArrayEquals(exceptResult,nums);

    }


}