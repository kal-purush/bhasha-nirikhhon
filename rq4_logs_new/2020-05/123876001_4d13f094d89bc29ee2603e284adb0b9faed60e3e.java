package practice.codeforces;

import java.util.Scanner;

/**
 * @Link https://codeforces.com/problemset/problem/1/B
 * status: ac
 * note: 1.数字转换的逻辑， 2. 模式判断的逻辑 3. 测试用例要丰富一些
 */
public class B_Spreadsheets {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        while (n-- > 0) {
            String tcase = sc.next();
            System.out.println(processing(tcase));
        }
    }


    private static String processing(String tcase) {
        char[] tcaseChar = tcase.toCharArray();
        String res = "";

        int locR = -1;
        int locC = -1;
        for (int k = 0; k < tcaseChar.length; k++) {
            if (tcaseChar[k] == 'R')
                locR = k;
            if (tcaseChar[k] == 'C')
                locC = k;
        }
        boolean RC_mode = false;
        if (locR == 0 && locC != -1 && locC-locR > 1) {
            RC_mode = true;
            for (int k = locR + 1; k < locC; k++) {
                if ((int)tcaseChar[k] >= (int)'A' && (int)tcaseChar[k] <= 'Z')
                    RC_mode = false;
            }
        }

        if (RC_mode) {
            //RXCY mode
            StringBuffer sb = new StringBuffer();
            int i = 1;
            while (i < tcaseChar.length && tcaseChar[i] != 'C') {
                sb.append(tcaseChar[i]);
                i++;
            }
            i++;
            int number = 0;
            int _10 = 1;
            for (int j=tcaseChar.length-1; j>=i; j--) {
                number = number + (tcaseChar[j] - '0') * _10;
                _10 *= 10;
            }
            res = convertNumberToRC(number) + sb.toString();

        } else {
            //RCXY mode
            StringBuffer sb = new StringBuffer();
            int i=0;
            while(i < tcaseChar.length && ((int)tcaseChar[i] >= (int)'A' && (int)tcaseChar[i] <= (int)'Z')) {
                sb.append(tcaseChar[i]);
                i++;
            }
            StringBuffer sb2 = new StringBuffer();
            sb2.append('R');
            for (;i < tcaseChar.length; i++) {
                sb2.append(tcaseChar[i]);
            }
            sb2.append('C');
            res = sb2.toString() + convertRCToNumber(sb.toString());
        }

        return res;
    }


    private static String convertNumberToRC(int number) {
        StringBuffer sb = new StringBuffer();
        int y = number;
        int remainder = 0;
        while (y > 26) {
            remainder = (y - 1) % 26 + 1;
            sb.append((char)(remainder + 64));
            y = (y - remainder) / 26;
        }
        sb.append((char)(y + 64));
        return sb.reverse().toString();
    }

    private static int convertRCToNumber(String rC) {
        char[] rcChars = rC.toCharArray();
        int res = 0;
        int _26 = 1;
        for (int i = rcChars.length - 1; i >= 0; i--) {
            res = res + ((int)rcChars[i] - 64)*_26;
            _26*=26;
        }
        return res;
    }
}