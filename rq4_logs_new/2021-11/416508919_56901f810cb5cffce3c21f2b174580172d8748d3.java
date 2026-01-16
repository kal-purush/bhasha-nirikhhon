package trees;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class KaryTree<T> {
    Knode<T> root;
    int k;

    public KaryTree(int maxChildNum){
        if (maxChildNum<=1)
            maxChildNum =2;

        this.k = maxChildNum;
    }

    public boolean isEmpty() {

        return root == null;
    }

    public void add(T value){
        Knode<T> newNode = new Knode<>(value);
        if(this.root == null){
           root = newNode;
            return;
        }

        Queue<Knode<T>> qNode =new LinkedList<>();
        qNode.add(root);
        while (!qNode.isEmpty()){

            Knode<T> current = qNode.poll();
            if (current.allChildren.size() < this.k ){
                current.addChild(newNode);
            return;

            }
            else {
                qNode.addAll(current.allChildren);
            }

        }


    }
    public static KaryTree<String> fizzBuzzTree(KaryTree<Integer> Ktree ){
        KaryTree<String> fizzBuzzValue = new KaryTree<>(Ktree.k);
        Queue<Knode<Integer>> newQueue = new LinkedList<>();


        if(Ktree.root != null){
            newQueue.add(Ktree.root);
            while (!newQueue.isEmpty()){
                Knode<Integer> current = newQueue.poll();

                String currentValue ="";
                if (current.value %3 ==0 && current.value %5 == 0){
                    currentValue = "FizzBuzzz";

                }else if (current.value %3 ==0){
                    currentValue = "Fizzz";

                }else if (current.value %5 ==0) {
                    currentValue = "Buzzz";
                }
                else {
                    currentValue=current.value.toString();
                }
                fizzBuzzValue.add(currentValue);
                if(! current.allChildren.isEmpty())
                    newQueue.addAll(current.allChildren);
            }


            }


        return fizzBuzzValue;

    }

    public List<T> breadthFirstSearch(){
        List <T> kTree = new ArrayList<>();
        Queue<Knode<T>> queue = new LinkedList<>();
        if(root != null){
            queue.add(root);
            while(!queue.isEmpty()){

                Knode<T> current = queue.poll();
                kTree.add(current.value);

                if(! current.allChildren.isEmpty())
                    queue.addAll(current.allChildren);
            }
        }
        return kTree;
    }

}