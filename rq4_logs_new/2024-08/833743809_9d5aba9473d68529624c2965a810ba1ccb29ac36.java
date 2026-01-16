public class Integer_to_English_words_273 {
    public static void main(String[] args) {
        int num=104000000;
        if (num == 0) {
            System.out.println("Zero");
        }
        int count = 0, i = num;
        String ans = "";
        while (i > 0) {
            count++;
            if (count == 2 && i%1000!=0 && i!=10000 && i!=100000 && i!=1000000) {
                ans = " Thousand " + ans;
            } else if (count == 3 && i!=1000) {
                ans = " Million " + ans;
            } else if (count == 4 ) {
                ans = " Billion " + ans;
            }
            ans = word(i % 1000) + ans;

            i = i / 1000;
        }
        System.out.println((ans.trim().replaceAll("\\s+", " ")));
    }
    public static String word(int num) {
        String[] ones = { "", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine" };
        String[] teens = { "Ten", "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen",
                "Eighteen", "Nineteen" };
        String[] tens = { "", "", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety" };
        if (num < 10) {
            return ones[num];
        } else if (num < 20) {
            return teens[num - 10];
        } else if (num < 100) {
            return tens[num / 10] + " " + ones[num % 10];
        }
        return ones[num / 100] + " Hundred " + word(num % 100);
    }
}