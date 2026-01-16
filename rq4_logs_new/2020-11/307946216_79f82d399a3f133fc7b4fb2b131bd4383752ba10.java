package hw.lesson6;

import java.util.Random;

public class HomeworkTreesTest {
    public static void main(String[] args) {

        Random random = new Random();

        int treesQuantity = 20;
        int levelsQuantity = 4;
        int maxValue = 25;

        int nodesQuantity = (int) (Math.pow(2, levelsQuantity) - 1);

        int balancedTreesQuantity = 0;

        boolean treeShow = false;

        for (int i = 0; i < treesQuantity; i++) {
            Tree<Integer> tree = new TreeImpl<>(levelsQuantity);
            generateTree(random, nodesQuantity, maxValue, tree);
            if (tree.isBalanced()) {
                balancedTreesQuantity++;
                if (!treeShow) {
                    treeShow = true;
                    tree.display();
                }
            }
        }
       System.out.println("Now we've got the following balanced trees percentage: "
                + ((balancedTreesQuantity / (treesQuantity * 1.0)) * 100) + " %");

    }

    private static void generateTree(Random random, int nodesQuantity, int maxValue, Tree<Integer> tree) {
        for (int i = 0; i < nodesQuantity; i++) {
            tree.add(random.nextInt(maxValue * 2 + 1) - maxValue);
        }
    }

}