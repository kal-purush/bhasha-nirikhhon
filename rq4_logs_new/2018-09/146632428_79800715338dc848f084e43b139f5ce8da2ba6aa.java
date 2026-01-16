import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class UseQueue
{
    public static void main(String[] args) {


        Queue<String> st = new LinkedList<String>();
        st.add("PA");
        st.add("LA");
        st.add("MI");

        //System.out.println(st.poll());
      for (Object state: st){
          System.out.println(state);
      }
        System.out.println("otherway");
      Iterator it=st.iterator();

        while (it.hasNext()) {
            System.out.println(it.next());
        }
        System.out.println("3rd way");
        //Using while as long as Linklist is not empty
        while (!st.isEmpty()){
            System.out.println(st.poll());
        }



    }


    }