package Easy;

public class SearchKeyInBST {
    private TreeNode root;

    public static void main(String[] args) {
        SearchKeyInBST bst = new SearchKeyInBST();

        bst.insert(5);
        bst.insert(3);
        bst.insert(7);
        bst.insert(1);

//        System.out.println(bst.isPresent(bst.root, 7).data);
        System.out.println(search(bst.root, 11));

    }
    static boolean res = false;

    public static boolean search(TreeNode root, int key){
        if(root == null){
            return res;
        }
        if( root.data == key ){
            res = true;
        }
        if(key < root.data){
            return search(root.left,key);
        } else {
            return search(root.right, key);
        }
    }
    public TreeNode isPresent(TreeNode root, int key){
        if(root == null || root.data == key ){
            return root;
        }
        if(key < root.data){
            return isPresent(root.left,key);
        } else {
            return isPresent(root.right, key);
        }
    }
    public void inOrder(){
        inOrder(root);
    }

    public void inOrder(TreeNode root){
        if(root==null){
            return;
        }
        inOrder(root.left);
        System.out.print(root.data+"->");
        inOrder(root.right);
    }

    private void insert(int value){
        root = insert(root, value);
    }

    private TreeNode insert(TreeNode root, int value){
        if(root == null){
            root = new TreeNode(value);
            return root;
        }
        if(value < root.data){
            root.left = insert(root.left,value);
        } else if(value > root.data){
            root.right = insert(root.right,value);
        }
        return root;
    }
    class TreeNode {
        private TreeNode left;
        private TreeNode right;
        private int data;

        public TreeNode(int data){
            this.data = data;
        }
    }

}