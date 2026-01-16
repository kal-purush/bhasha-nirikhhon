import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.*;

public class Main {

    public static class Node{
        int value;
        int priority, size;
        Node left, right;

        Node(int value){
            this.value = value;
            priority = (int) (Math.random()*100000);
            size = 1;
            left = null;
            right = null;
        }

        void setLeft(Node left){
            this.left = left;
            calcSize();
        }

        void setRight(Node right){
            this.right = right;
            calcSize();
        }

        void calcSize() {
            size = 1;
            if(left != null) size += left.size;
            if(right != null) size += right.size;
        }
    }

    public static class PairNode{
        Node prev;
        Node next;

        PairNode(Node prev, Node next) {
            this.prev = prev;
            this.next = next;
        }
    }

    static PairNode split(Node root, int value){
        if(root == null)
            return new PairNode(null, null);
        if(root.value < value){
            PairNode pairNode = split(root.right, value);
            root.setRight(pairNode.prev);
            return new PairNode(root, pairNode.next);
        }
        PairNode pairNode = split(root.left, value);
        root.setLeft(pairNode.next);
        return new PairNode(pairNode.prev, root);
    }

    static Node merge(Node a, Node b){
        if(a == null) return b;
        if(b == null) return a;
        if(a.priority < b.priority){
            b.setLeft(merge(a, b.left));
            return b;
        }
        a.setRight(merge(a.right, b));
        return a;
    }

    static Node insert(Node root, Node target){
        if(root == null) return target;
        if(root.priority < target.priority){
            PairNode splitted = split(root, target.value);
            target.setLeft(splitted.prev);
            target.setRight(splitted.next);
            return target;
        }
        else if (target.value < root.value)
            root.setLeft(insert(root.left, target));
        else
            root.setRight(insert(root.right, target));
        return root;
    }

    static Node erase(Node root, int value){
        if(root == null) return root;
        if(root.value == value){
            Node rst = merge(root.left, root.right);
            return rst;
        }
        if(value < root.value)
            root.setLeft(erase(root.left, value));
        else
            root.setRight(erase(root.right, value));
        return root;
    }

    static Node findKthNode(Node root, int k){
        int leftSize = 0;
        if(root.left != null) leftSize = root.left.size;
        if(k <= leftSize) return findKthNode(root.left, k);
        if(k == leftSize+1) return root;
        return findKthNode(root.right, k-leftSize-1);
    }

    static int countLessThan(Node root, int value){
        if(root == null) return 0;
        if(root.value >= value)
            return countLessThan(root.left, value);
        int leftSide = (root.left != null) ? root.left.size : 0;
        return leftSide+1+countLessThan(root.right, value);
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int c = Integer.parseInt(br.readLine());

        for(int t = 0; t < c; t++){
            int n = Integer.parseInt(br.readLine());
            StringTokenizer st = new StringTokenizer(br.readLine());
            int[] shifted = new int[n];
            int[] rst = new int[n];
            for(int i = 0;i < n; i++){
                shifted[i] = Integer.parseInt(st.nextToken());
            }

            Node candidates = null;
            for(int i = 0; i < n; i++)
                candidates = insert(candidates, new Node(i+1));
            for(int i = n-1; i >= 0; i--){
                int larger = shifted[i];
                Node k = findKthNode(candidates, i+1-larger);
                rst[i] = k.value;
                candidates = erase(candidates, k.value);
            }
            for(int i= 0 ;i < n; i++){
                System.out.print(rst[i]+" ");
            }
            System.out.println();
        }
    }
}