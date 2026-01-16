/**
 * You're given strings jewels representing the types of stones that are jewels, and stones representing the stones you have.
 * Each character in stones is a type of stone you have. You want to know how many of the stones you have are also jewels.
 * Letters are case sensitive, so "a" is considered a different type of stone from "A".
 *
 * Example 1:
 * Input: jewels = "aA", stones = "aAAbbbb"
 * Output: 3
 *
 * Example 2:
 * Input: jewels = "z", stones = "ZZ"
 * Output: 0
 */

public class JewelAndStones {
    public static void main(String[] args) {
        System.out.println("The number of jewels in the stone 'aAAbbbb' are: "+ new JewelAndStones().numJewelsInStones("aA", "aAAbbbb"));
        System.out.println("The number of jewels in the stone 'aAAbbbb' are: "+ new JewelAndStones().numJewelsInStones("z", "ZZ"));
    }

    public int numJewelsInStones(String jewels, String stones) {
        // Your code goes here ...
        return 0;
    }
}