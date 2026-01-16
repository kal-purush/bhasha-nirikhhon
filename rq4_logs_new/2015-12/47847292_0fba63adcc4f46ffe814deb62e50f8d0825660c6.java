import net.sf.javabdd.BDD;
import net.sf.javabdd.BDDFactory;
import net.sf.javabdd.BDDPairing;
import net.sf.javabdd.MicroFactory;

import java.util.List;

/**
 * Created by ginva_000 on 04.12.2015.
 */
public class Example {
    public static void main(String[] args) {
        BDDFactory factory = MicroFactory.init(1000, 100);
        factory.setVarNum(6);
        BDD a1 = factory.ithVar(0);
        BDD a2 = factory.ithVar(1);
        BDD a3 = factory.ithVar(2);
        BDD b1 = factory.ithVar(3);
        BDD b2 = factory.ithVar(4);
        BDD b3 = factory.ithVar(5);
        BDD z = a1.and(b1).or(a2.and(b2)).or(a3.and(b3));
        int c = z.nodeCount();
        System.out.println(c);
        z.printDot();
        factory.swapVar(1,3);
        factory.swapVar(2,1);
        factory.swapVar(2,4 );
//        System.out.println(z.nodeCount());
//        System.out.println(z.satCount());
        z.printSet();
        int[] m = z.scanSet();
        System.out.println(m.length);
        for (int i:m) {
            System.out.println(i);
        }
        List<byte[]> list = z.allsat();
        for(byte[] arr : list){
            for (byte b: arr) {
                if(b >= 0) System.out.print(b);
                else System.out.print("X");

            }
            System.out.println("");
        }

        System.out.println(z);
        BDD z1 = z.satOne();
        System.out.println(z1);
        BDD z2 = z.satOne(b1.and(b2).and(b3), true);
//        z1.printDot();
        System.out.println(z2);
        BDD ze = z.exist(b1.and(b2).and(b3));
        System.out.println(ze);
//        ze.printDot();
        BDDPairing pairs = factory.makePair();
        int[] a = {0,1,2};
        int[] b = {3,4,5};
        pairs.set(a,b);
        BDD zpair = ze.replace(pairs);



    }
}