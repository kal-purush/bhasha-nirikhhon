package prop13.graphlibrary;

/**
 * Created by miquel on 20/03/15.
 */
public final class TEdge extends Edge
{
    public TEdge()
    {
        System.out.println("Sorry buddy, there is no default construcor.Check your code for errors");
    }
     public TEdge(TNode m1,TNode m2,int w)
    {
        n1 = m1;
        n2 = m2;
        weight = w;
    }

    public void printEdge()
    {
        System.out.print("(");
        ((TNode)n1).printNode();
        System.out.print(",");
        ((TNode)n2).printNode();
        System.out.print(")");
    }
}