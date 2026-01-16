import java.util.*;

class Node {
    int data;
    Node left, right;
    public Node(int data) {
        this.data = data;
    }
}

public class data_Structrue {
    public Node root = null;
    public void createNode(int data, int leftData, int rightData) {
        root = new Node(data);
        if(leftData != -10000){
            root.left = new Node(leftData);
        }
        if(rightData != -10000){
            root.right = new Node(rightData);
        }
    }

    public void inorderTraversal(Node root) {
        if(root != null){
            inorderTraversal(root.left);
            System.out.print(root.data + " ");
            inorderTraversal(root.right);
        }
    }

    public int scan(){
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter the data: ");
        int data = sc.nextInt();
        return data;
    }

    public static void main(String[] args) {
        data_Structrue obj = new data_Structrue();

        obj.createNode(obj.scan(), obj.scan(), obj.scan());

        System.out.println("중위 순회");
        obj.inorderTraversal(obj.root);
    }

}

/*

dfs
    >> depth first search
       루트 노드에서 가장 왼쪽 잎 노드까지 내려간 뒤
       가장 가까운 분기로 돌아와 그 분기에서의 다른 노드의 자식들로 내려가며 탐색을 수행함
       스택 또는 재귀함수를 이용하여 구현 가능

bfs
    >> breadth first search
    루트 노드에 인접한 노드부터 잎 노드 순 으로 탐색하는 방법
    큐를 사용하여 구현 가능

 */