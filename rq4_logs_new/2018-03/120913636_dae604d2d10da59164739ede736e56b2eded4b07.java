class Recurs {
    int recur(int n) {
        int result;

        if (n==1) return 1;
        result = recur (n-1) * n;
        return result;

    }
    void viewer (int n){
        int[] view = new int[n-1];

        for (int i =0; i < view.length ; i++) {
            view[i] = n;
            n--;
            System.out.print (view[i]);
            if (i <view.length-1) System.out.print (" + ");

        }
    }
}
public class Recursion {
    public static void main (String ars[]){
        Recurs rec = new Recurs ();

        int n = 5;
        System.out.print("Silnia z " + n +": ");
        rec.viewer (n);
        System.out.print (" = " + rec.recur (n));

    }

}