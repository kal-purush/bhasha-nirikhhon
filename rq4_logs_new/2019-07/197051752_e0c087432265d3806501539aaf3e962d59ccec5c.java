public class BST {
    private Node root;
    private Node current;
    private int tree_size;                          //keeps track of the length of the tree

    public BST(){                                   //constructor for empty BST
        root=null;
        tree_size=0;
        }

    public BST (int data){                          //constructor for BST with 1 element
        root=new Node(data);
        tree_size=1;
    }

    public Node head(){                             // returns reference to root node
        return root;
    }

    public int data(){                              // returns value at the root node
        return root.getData();
    }

    public int length(){                           // rerturns length of the BST
        if(root==null)
            return 0;
        else
        return tree_size;
    }

    public void insert(int entry){              ///inserts entry into BST in its correct position
        if (root==null){
            root=new Node(entry);
            tree_size++;
        }
        else {
            current=root;
            while(current!=null){
                if(current.getData() >= entry){
                    if(current.getLeft()==null) {
                        current.setLeft(entry);
                        current=null;
                    } else
                        current=current.getLeft();
                }
                else {
                    if (current.getRight() == null){
                        current.setRight(entry);
                        current=null;}
                    else
                        current=current.getRight();
                    }
            }
        }
        tree_size++;
    }

    public Node search(Node root,int target){               // searches tree for a node with the value of target
        if(root==null || root.getData()==target)
            return root;

        if (root.getData()> target)
            return search(root.getLeft(), target);

        return search(root.getRight(), target);
        }

    public void remove(int target){                       // initializes the deletion of a node with value target
        root= delete(root,target);
    }

    public Node delete(Node root, int target){           // searches for node with target recursively and removes it
        if (root == null){
            return root;
        }
        if(target < root.data) {
            root.left=delete( root.left, target);
        }
            else if (target > root.data) {
               root.right =delete(root.right, target);
            }
            else {
                if (root.left==null)
                    return root.right;
                else if (root.right==null)
                    return root.left;
                root.data = min_element(root.right);
                root.right=delete(root.right,root.data);
                }

                return root;
    }

    public int min_element(Node root){             // returns the min element in the BST
        if(root.left==null)
            return root.data;
        else {
            return min_element(root.left);
        }
    }

}
