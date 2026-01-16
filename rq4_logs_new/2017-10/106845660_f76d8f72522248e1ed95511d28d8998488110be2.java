
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 *  @author L�r�nt Mikol�s
 */

public class AVLTree<T> {
    private Node<T> root;
    private Comparator<T> cmp;

    public AVLTree(Comparator<T> cmp){
        this.cmp = cmp;
    }

    private static class Node<T> implements PrintableNode {
        private Node<T> left;
        private Node<T> right;
        private T key;
        private int height = 0;
        /*indexes of the BlockChain's blocks  that affected this node*/
        private Set<Integer> modIndex = new HashSet<>();

        public Node(T key) {
            height = 0;
            left = null;
            right = null;
            this.key = key;
        }

        public Node(T key, int index){
            this(key);
            this.modIndex.add(index);
        }

        public Node(T key, Node<T> left, Node<T> right) {
            this.left = left;
            this.right = right;
            this.key = key;
        }

        @Override
        public String toString(){
            return  key.toString();
        }

        public int getHeight(){
            return height;
        }

        public Node<T> getLeft() {
            return left;
        }

        public Node<T> getRight() {
            return right;
        }

        public int getRightChildHeight(){
            if(right == null){
                return -1;
            }
            return right.height;
        }

        public int getLeftChildHeight(){
            if(left == null){
                return -1;
            }
            return left.height;
        }
    }

    public Node<T> getRoot() {
        return root;
    }

    /**
     * Adds a new element to the AVLTree.
     * This method serves as a wrapper.
     * @param key element to add.
     * @param blockIndex index of the BlockChain's block that has the add operation.
     * @return boolean that is true if element is added or false otherwise
     */
    public boolean add(T key, int blockIndex) {
        Node<T> aux = add(key, root, blockIndex);
        if(aux == null) return false;
        root = aux;
        return true;
    }

    /**
     * Adds a new element to the AVLTree recursively. Checks balance, updates height and
     * and updates the indexes of the blocks that affected the node on the way back of the
     * recursion.
     * @param key element to add.
     * @param current current element in the recursion.
     * @param blockIndex index of the BlockChain's block that has the add operation.
     * @return child node in recursion or null if key is already present in the AVLTree.
     */
    private Node<T> add(T key, Node<T> current, int blockIndex) {
        Node<T> aux;
        /*if the leaf is reached, add a new node*/
        if(current == null){
            return new Node<>(key, blockIndex);
        }
        /*advance through left child*/
        if(cmp.compare(key,current.key) < 0 ){

            aux = add(key, current.left, blockIndex); //calls recursive method.
            if(aux == null) return null;
            if(aux != current.left) current.modIndex.add(blockIndex);
            current.left = aux;
            /*update current nodes height*/
            current.height = Math.max(current.height, current.getLeftChildHeight() + 1);
        }
        /*advance through right child*/
        else if(cmp.compare(key,current.key) > 0 ){

            aux = add(key, current.right, blockIndex); //calls recursive method.
            if(aux == null) return null;
            if(aux != current.right) current.modIndex.add(blockIndex);
            current.right = aux;
            /*update current nodes height*/
            current.height = Math.max(current.height, current.getRightChildHeight() + 1);
        }
        else {
            return null;
        }
        current = balance(current, blockIndex);
        return current;
    }

    /**
     * Checks balance of the node.
     * @param current Node that is verified for a correct factor of balance.
     * @param blockIndex index of the BlockChain's block that has the add operation. It's used
     *                   in case  a rotation is needed.
     * @return a new node in case the current was affected by a rotation.
     */
    public Node<T> balance(Node<T> current, int blockIndex){
        int balance = getBalance(current);

        /*checks FB*/
        if(balance > 1){
            /*left left*/
            if(getBalance(current.left) >= 0){
                current = rightRotation(current, blockIndex);

            }
            /*left right*/
            else{
                current.left = leftRotation(current.left, blockIndex);
                current = rightRotation(current, blockIndex);
            }
        }
        /*checks FB*/
        else if(balance < -1){

            /*right right*/
            if(getBalance(current.right) <= 0) {
                current = leftRotation(current, blockIndex);
            }
            /*right left*/
            else{
                current.right = rightRotation(current.right, blockIndex);
                current = leftRotation(current, blockIndex);
            }
        }
        return current;
    }

    /**
     * Performs a left rotation to the current node
     * @param current node to be rotated.
     * @param blockIndex index of the BlockChain's block that has the add operation. Every node
     *                   affected by this rotation will update the indexes of the blocks that modified said node.
     * @return the new root of the subtree.
     */
    private Node<T> leftRotation(Node<T> current, int blockIndex){
        Node<T> auxright = current.right;
        current.right = auxright.left;
        auxright.left = current;
        current.height = Math.max(current.getLeftChildHeight() + 1, current.getRightChildHeight() + 1);
        auxright.height = Math.max(auxright.height, current.height + 1);
        current.modIndex.add(blockIndex);
        auxright.modIndex.add(blockIndex);
        if(current.right != null) current.right.modIndex.add(blockIndex);
        return auxright;
    }
    /**
     * Performs a right rotation to the current node
     * @param current node to be rotated.
     * @param blockIndex index of the BlockChain's block that has the add operation. Every node
     *                   affected by this rotation will update the indexes of the blocks that modified said node.
     * @return the new root of the subtree.
     */
    private Node<T> rightRotation(Node<T> current, int blockIndex){
        Node<T> auxleft = current.left;
        current.left = auxleft.right;
        auxleft.right = current;
        current.height = Math.max(current.getLeftChildHeight() + 1, current.getRightChildHeight() + 1);
        auxleft.height = Math.max(auxleft.height, current.height + 1);
        current.modIndex.add(blockIndex);
        auxleft.modIndex.add(blockIndex);
        if(current.left != null) current.left.modIndex.add(blockIndex);
        return auxleft;
    }

    /**
     * Calculates the height diference between left and right child. An Empty AVLTree is considered
     * balanced.
     * @param current Node to get balance from.
     * @return FB.
     */
    public int getBalance(Node<T> current){
        if(current == null) return 0;
        return current.getLeftChildHeight() - current.getRightChildHeight();
    }

    /**
     * Removes a key from the AVLTree.
     *  A DataPair is used to allow for two return values for the remove method.
     * @param key element to be removed from AVLTree.
     * @param blockIndex index of the BlockChain's block that has the remove operation.
     * @return true if removal was successful or false otherwise.
     */
    public boolean remove(T key, int blockIndex){
        DataPair<Boolean,Node<T>> aux = remove(key, root, blockIndex);
        root = aux.getElement2();
        return aux.getElement1();

    }
    /**
     * Removes a key from the AVLTree recursively. Checks balance, updates height and
     * and updates the indexes of the blocks that affected the node on the way back of the
     * recursion.
     * This method serves a wrapper. A DataPair is used to allow for two return values for the remove method.
     * @param key element to be removed from AVLTree.
     * @param blockIndex index of the BlockChain's block that has the remove operation.
     * @return a DataPair. The element1 of the DataPair indicates if the removal was successful or not with a
     * boolean and element2 is the new child returned in the recursion.
     */
    private DataPair<Boolean, Node<T>> remove(T key, Node<T> current, int blockIndex){
        /*if the element was not found returns false (unsuccessfull removal)*/
        if(current == null){
            return new DataPair<>(false,null);
        }
        DataPair<Boolean,Node<T>> aux;
        Node<T> node;
        /*advance through left child*/
        if(cmp.compare(key, current.key) < 0){
            aux = remove(key, current.left, blockIndex);
            node = aux.getElement2();
            if(current.left != node) current.modIndex.add(blockIndex);
            current.left = node;
        }
        /*advance through right child*/
        else if(cmp.compare(key, current.key) > 0){
            aux = remove(key, current.right, blockIndex);
            node = aux.getElement2();
            if(current.right != node) current.modIndex.add(blockIndex);
            current.right = node;
        }
        /*element found*/
        else {
            current = deleteKey(current, blockIndex);
            current = balance(current, blockIndex);
            return new DataPair<>(true, current);
        }
        /*if removal was successful, updates height and checks balance on the way back*/
        if(aux.getElement1()) {
            current.height = Math.max(current.getLeftChildHeight() + 1, current.getRightChildHeight() + 1);
            current = balance(current, blockIndex);
        }
        return new DataPair<>(aux.getElement1(), current);
    }

    /**
     * Removes key from the AVLTree in different ways depending on the right and left child.
     * @param node  to be removed from the AVLTree/
     * @param blockIndex index of the BlockChain's block that has the remove operation.
     * @return the new node or null that will take the place of the removed node.
     */

    public Node<T> deleteKey( Node<T> node, int blockIndex) {
        /*no childs*/
        if (node.right == null && node.left == null) {
            return null;
        /*has just left child*/
        } else if (node.right == null) {
            return node.left;
        /*has just right child*/
        } else if (node.left == null) {
            return node.right;
        /*search for the successor inorder*/
        } else {
            DataPair<Node<T>, Node<T>> aux = eliminateMostLeft(node.right, blockIndex);
            Node<T> ret = aux.getElement2();
            /*it may occur that the inorder successor is the right child*/
            if(ret == node.right) ret.right = null;
            else ret.right = aux.getElement1();
            ret.left = node.left;
            ret.height = Math.max(ret.getLeftChildHeight() + 1, ret.getRightChildHeight() + 1);
            ret.modIndex.add(blockIndex);
            return ret;
        }
    }

    /**
     * Removes the successor inorder from its current position and returns it recursively.  Checks balance, updates height and
     * and updates the indexes of the blocks that affected the node on the way back of the
     * recursion.
     * @param current node in the recursion.
     * @param blockIndex index of the BlockChain's block that has the remove operation.
     * @return a DataPair in which the element1 is the new child in recursion and element2 is the succesor inorder.
     */
    private DataPair<Node<T>,Node<T>> eliminateMostLeft(Node<T> current, int blockIndex){
        if(current == null){
            throw new NoSuccesorInorderException("There was no succesor inorder.");
        }
        DataPair<Node<T>, Node<T>> aux;
        if(current.left != null){
            /*call recursion*/
            aux = eliminateMostLeft(current.left, blockIndex);
            if(current.left != aux.getElement1()){ current.modIndex.add(blockIndex);

            }
            current.left = aux.getElement1();
        } else {
            return new DataPair<>(null, current);
        }
        current.height = Math.max(current.getLeftChildHeight() + 1, current.getRightChildHeight() + 1);
        current = balance(current, blockIndex);
        return new DataPair<>(current, aux.getElement2());
    }





    public List<T> getInRange(T inf, T sup) {
        List<T> result = new LinkedList<>();
        getInRange(root, result, inf, sup, cmp);
        return result;
    }

    private void getInRange(Node<T> current, List<T> result, T inf, T sup, Comparator<T> cmp) {
        if (current == null) {
            return;
        }
        if (cmp.compare(inf, current.key) < 0 && cmp.compare(inf, current.key) > 0) {
            result.add(current.key);
        }
        getInRange(current.right, result, inf, sup, cmp);
        getInRange(current.left, result, inf, sup, cmp);
        return;
    }
    public void print(){
        printNodesByLevel();

    }




    public void printNodesByLevel() {
        Deque<Node<T>> queue = new LinkedList<>();
        if (root == null){
            return;
        }
        queue.offer(root);
        int i = 0 ;
        double number = 1;
        while (!queue.isEmpty()) {
            Node<T> aux = queue.remove();
            if(	Math.log(number)/Math.log(2.0) == i ){
                System.out.println(" LEVEL + 1 = " + Math.log(number)/Math.log(2.0) );
                i++;
            }
            if (aux != null) {
                queue.offer(aux.left);
                queue.offer(aux.right);
                System.out.print( aux.toString() + "    ");
            } else {
                System.out.print("-EMPTY-    ");
            }
            number++;
        }
    }



    public static <T> int getHeight(Node<T> current) {
        if (current == null) return -1;
        return 1 + Math.max(getHeight(current.left), getHeight(current.right));
    }


    /**
     * Searches for key in the Tree and returns modIndex set.
     * This method serves as a wrapper
     * @param key to be searched in tree.
     * @return a DatarPair in which element1 is a Boolean (true if key was found, false otherwise)
     * and a Set of Integers with the indexes of the blocks that modified that node.
     */
    public DataPair<Boolean,Set<Integer>> lookup(T key) {
        return lookup(key,root);
    }

    /**
     * Searches for key in the Tree and returns modIndex set recursively.
     * @param key to be searched in tree.
     * @return a DatarPair in which element1 is a Boolean (true if key was found, false otherwise)
     * and a Set of Integers with the indexes of the blocks that modified that node.
     */
    private DataPair<Boolean,Set<Integer>> lookup(T key, Node<T> current) {
        if (current == null)
             return new DataPair<>(false, null);
        if (cmp.compare(key, current.key) < 0) {
            return lookup(key, current.left);
        }
        if (cmp.compare(key, current.key) > 0) {
            return lookup(key, current.right);
        }
        return new DataPair<>(true, current.modIndex);
    }




    public int getLevel(T key) {
        return getLevel(key, this.root, 0, this.cmp);
    }

    private int getLevel(T key, Node<T> current, int level, Comparator<T> cmp) {
        if (current == null) return -1;
        if (current.key.equals(key)) return level;

        if (current.right != null && cmp.compare(key, current.key) > 0)
            return getLevel(key, current.right, level + 1, cmp);
        if (current.left != null && cmp.compare(key, current.key) < 0)
            return getLevel(key, current.left, level + 1, cmp);
        return -1;
    }

    public int getLeavesCount() {
        return getLeavesCount(this.root);
    }

    public int getLeavesCount(Node<T> current) {
        if (current == null) return 0;
        int aux = getLeavesCount(current.left) + getLeavesCount(current.right);
        if (aux == 0)
            return 1;
        return aux;
    }

    public T getMax() {
        return getMax(this.root);
    }

    private T getMax(Node<T> current) {
        if (current == null) {
            return null;
        }
        if (current.right != null) {
            return getMax(current.right);
        }
        return current.key;
    }

    public void printDescendants(Node<T> node) {
        printDescendants(root, node, false);

    }

    private void printDescendants(Node<T> current, Node<T> node, boolean descendant) {
        if (current == null) {
            return;
        }
        if (descendant && current.equals(node)) {
            printDescendants(current.right, node, true);
            printDescendants(current.left, node, true);
        }
        if (descendant) {
            System.out.println(current.toString());
        }
    }

    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof AVLTree)) {
            return false;
        }
        AVLTree<T> bst = (AVLTree<T>) o;
        return equals(root, bst.root);
    }

    private boolean equals(Node<T> current, Node<T> other) {
        if (current == null && other == null)
            return true;
        boolean right, left;
        if (current.key.equals(other.key) && current.height == other.height) {
            return equals(current.right, other.right) && equals(current.left, other.left);
        }
        return false;

    }

    public int hashCode() {
        return hashCode(root);
    }

    private int hashCode(Node<T> current) {
        if (current == null)
            return 1;
        return 31 * current.key.hashCode() + hashCode(current.right) + hashCode(current.left);
    }


    public int size() {
        return size(root);
    }

    private int size(Node<T> current) {
        if (current == null) return 0;
        return 1 + size(current.right) + size(current.left);
    }
    
    public void clearTree() {
    	this.root = null;
    }

}