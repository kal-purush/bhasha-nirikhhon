/**
 *  グレゴリオ暦の閏年判定 - 乱数版
 */
public class exercises04 {
        public static void main(String[] args) throws Exception {
        int seireki = (int)(Math.random() * 3000 + 1);

        if (seireki % 400 == 0) {
            System.out.println(seireki + " leap year");
        } else if (seireki % 100 == 0) {
            System.out.println(seireki);
        } else if (seireki % 4 == 0) {
            System.out.println(seireki + " leap year");
        } else { // それ以外のとき
            System.out.println(seireki);
        }
    }
}