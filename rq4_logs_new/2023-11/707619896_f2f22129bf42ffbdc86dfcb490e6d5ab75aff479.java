package homeworks;

public class Practice {
    public static void main(String[] args) {
        int a = 3;
        int b = 2;
        a = b;
        System.out.println(a * b);

        int v = 5;
        int f = 10;
        v = v + ++f;
        System.out.println(v + f);

        int x = 1;
        int y = 1;
        int z = 0;
        if (!(x++ > 1 & y++ > 1)) {
            z = x + y;
        }
        System.out.println(z);

        int la = 4;
        for (int i = 2; i <= 5; i++) {
            if (i % 2 == 0) {
                la = la + i;
            }
        }
        System.out.println(la);

        int xa = 1;
        int ya = ++xa;
        System.out.println(ya);
        System.out.println(xa);
        ya += xa;
        System.out.println(ya);
        System.out.println(xa);


        String str = "Maks good coder!";
        int s = str.length();
        switch (s) {
            case (14):
                System.out.println(++s * 2);
                break;
            case (16):
                System.out.println(s++ * 2);
                break;
            default:
                System.out.println(s);
                break;
        }


        int ko = 1;
        int po = 2;
        for (int i = 1; i < 10; i++) {
        }
        System.out.println(ko + "po");

        int[] arr = {8, 3, 2};
        for (int i = 1; i < 2; i++) {
            arr[0] *= arr[i];
        }
        System.out.println(arr[0]);

        for (int j = 0; j < 5; j++) {
            switch (j) {
                case (0):
                case (2):
                case (4):
                    System.out.println(j + 1);
                    break;

            }
        }

        int fh = 16;
        int fj = 3;
        fj += ++fh;
        System.out.println(fj++);

        int t = 1;
        for(int i = 1; i < 5; i++) {
            System.out.println(t + 1);
        }

        int[] iop = {1, 2, 3, 4, 5, 6};
        int fhfy = iop.length - 1;
        while (fhfy >=0) {
            System.out.println(iop[fhfy]);
            fhfy--;
        }

        int u = 5;
        switch (u) {
            case 1:
                ++u;
            case 5:
                ++u;
            default:
                ++u;
        }
        System.out.println(++u);
        System.out.println(--u);

        String s1 = "a1";
        String s2 = "a" + 1;
        System.out.println(s2.equals(s2));

        int tr;
        int o = 12;
        for(int p = 1; p < 10; p ++) {
            o--;
        }
        System.out.println(o);
    }
}