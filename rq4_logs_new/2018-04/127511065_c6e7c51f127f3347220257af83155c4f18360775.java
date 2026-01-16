import Search.TabuSearch;
import model.FileOperation;
import model.Vex;

/**
 * Created by CaiNot on 2018/3/20.
 */
public class GraphColor {

    public static void main(String[] args) {
//        Scanner scanner=new Scanner(System.in);

        final String PATH = "D:/instances/instances/DSJC125.1.col";

        FileOperation fileOperation = new FileOperation();
        int[][] graph = fileOperation.readFile(PATH);
        if (graph == null) {
            System.out.println("Error file read error");
            System.exit(1);
        }

        TabuSearch search = new TabuSearch();
        int k = 20;
        int loop = 0;
        Vex[] vexes = null;
        int position = 0;

        while (k > 0) {
            search.init(graph, k);
            loop = 0;
            position = 0;
            vexes = search.getGraph().vexes;

            while (search.getGraph().calcEnemyValue() != 0) {
//                if (vexes[position].calcEnemyValue() != 0) {
                loop++;

                if (loop > 100000) {
                    System.exit(1);
                }
//                for (int i = 0; i < vexes.length; i++) {
                search.move(vexes[loop % vexes.length], loop);
//                }
            }
            System.out.println("Color is " + String.valueOf(k));
            k--;
        }
    }
}