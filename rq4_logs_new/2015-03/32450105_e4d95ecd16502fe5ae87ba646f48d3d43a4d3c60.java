
import prop13.graphlibrary.*;
/**
 * Created by miquel on 20/03/15.
 */
public class Main
{
    public static void main(String[] args)
    {
        System.out.println("Hello World!");

        Node n1 = new Node();
        Node n2 = new Node();
        Edge e = new Edge(n1,n2,1);

        //GetNeighbor Testing and functionality showcase
        System.out.println("n1: "+n1);
        System.out.println("n2: "+n2);
        System.out.println("n1 neighbor: "+ e.getNeighbor(n1));
        System.out.println("n2 neighbor: "+e.getNeighbor(n2));

    }
}