import java.util.Vector;

public class BinarySearchTree <K extends Comparable<? super K>, V> extends Vector<TreeItem<Integer, String>> {

	private TreeNode<K, V> root;

	/**
	 * Default constructor for BinarySearchTree. This constructor instantiates
	 * an empty tree
	 */
	public BinarySearchTree() {
		this.root = null;
	}


	/**
	 * This constructor instantiates a BinarySearchTree with the given <code>root</code>
	 *
	 * @param root The root TreeNode for this BinarySearchTree
	 */
	public BinarySearchTree(TreeNode<K, V> root) {
		this.root = root;
	}


	/**
	 * Method to get the current root of this BinarySearchTree
	 *
	 * @return The root TreeNode for this BinarySearchTree
	 */
	public TreeNode<K, V> getRoot() {
		return root;
	}


	/**
	 * Method to set set the root of this BinarySearchTree
	 *
	 * @param root The new root of this BinarySearchTree
	 */
	public void setRoot(TreeNode<K, V> root) {
		this.root = root;
	}


	/**
	 * Method to get the <b>TreeItem</b> currently in the root of this BinarySearchTree
	 *
	 * @return Returns the <b>TreeItem</b> currently in the root
	 * @throws TreeException Throws a <b>TreeException</b> if the root of the tree is <b>null</b>
	 */
	public TreeItem<K, V> getRootItem() throws TreeException {
		if (this.root == null) {
			throw new TreeException("TreeException: Tree Is Empty, No Root Item");
		} else {
			return this.root.getTreeItem();
		}
	}


	/**
	 * Method to find out if the BinarySearchTree is empty
	 *
	 * @return Returns <b>true</b> if the root in <b>null</b>, otherwise, returns <b>false</b>
	 */
	public boolean isEmpty() {
		return (root == null);
	}


	/**
	 * Method to remove all entries from the BinarySearchTree.
	 */
	public void makeEmpty() {
		this.root = null;
	}


	/**
	 * Method to find and retrieve the <b>TreeItem</b>
	 * with the given <b>key</b> if it is in the BinarySearchTree.
	 *
	 * @param key The <b>key</b> that the user wishes to search for
	 * @return The <b>TreeItem</b> with the given <b>key</b> if found. Otherwise, returns
	 * <b>null</b>
	 */
	public TreeItem<K, V> find(K key) {
		return findItem(this.root, key);
	}


	/**
	 * The <b>private recursive</b> method to find and retrieve  the <b>TreeItem</b>
	 * with the given <b>key</b> if it is in the BinarySearchTree. This method is initially
	 * called by the {@link #find <b><u>find</u></b>} method
	 *
	 * @param node The current TreeNode being searched
	 * @param key  The <b>key</b> that the user wishes to search for
	 * @return The <b>TreeItem</b> with the given <b>key</b> if found. Otherwise, returns
	 * <b>null</b>
	 */
	private TreeItem<K, V> findItem(TreeNode<K, V> node, K key) {
		if (node == null) {
			return null;
		} else if (node.getTreeItem().getKey().compareTo(key) == 0) {
			return node.getTreeItem();
		} else if (node.getTreeItem().getKey().compareTo(key) > 0) {
			return findItem(node.getLeftChild(), key);
		} else {
			return findItem(node.getRightChild(), key);
		}
	}


	/**
	 * Method to insert the given <b>TreeItem</b> into the BinarySearchTree
	 *
	 * @param treeItem The <b>TreeItem</b> to insert into the BinarySearchTree
	 */
	public void insert(TreeItem<K, V> treeItem) {
		this.root = insertItem(this.root, null, treeItem);
	}


	/**
	 * The <b>private recursive</b> method to insert the <b>TreeItem</b>
	 * into the BinarySearchTree. This method is initially
	 * called by the {@link #insert <b><u>insert</u></b>} method
	 *
	 * @param node     The current TreeNode being examined for insertion
	 * @param parent   The current parent of the TreeNode being examined
	 * @param treeItem The <b>TreeItem</b> to insert into the BinarySearchTree
	 * @return The node that existed or was inserted into the BinarySearchTree
	 */
	private TreeNode<K, V> insertItem(TreeNode<K, V> node, TreeNode<K, V> parent, TreeItem<K, V> treeItem) {
		if (node == null) {
			node = new TreeNode<K, V>(treeItem);
			node.setParent(parent);
		} else if (node.getTreeItem().getKey().compareTo(treeItem.getKey()) > 0) {
			node.setLeftChild(this.insertItem(node.getLeftChild(), node, treeItem));
		} else {
			node.setRightChild(this.insertItem(node.getRightChild(), node, treeItem));
		}

		return node;
	}


	/**
	 * This is the public method used to delete a TreeItem from the <code>BinarySearchTree</code> based
	 * on the specified <b>key</b>.
	 *
	 * @param key The <b>key</b> of the TreeItem the user wishes to delete from the <code>BinarySearchTree</code>.
	 */
	public void delete(K key) throws TreeException {
		setRoot(deleteItem(getRoot(), key));
	}   // end delete


	/**
	 * The <b>private recursive</b> method to delete the <b>TreeItem</b>
	 * with the specified <b>key</b> from BinarySearchTree. This method is initially
	 * called by the {@link #delete <b><u>delete</u></b>} method
	 *
	 * @param node The current TreeNode being examined for deletion
	 * @param key  The <b>key</b> of the TreeItem to be deleted
	 * @return The TreeNode reference that will replace the deleted TreeNode
	 */
	private TreeNode<K, V> deleteItem(TreeNode<K, V> node, K key) {

		if (node == null) {
			throw new TreeException("TreeException:  Item not found");
		} else {
			TreeItem<K, V> treeItem = node.getTreeItem();
			if (key.compareTo(treeItem.getKey()) == 0) {
				// item is in this node, which is the root of a subtree
				node = deleteNode(node);   // delete the item
			} else if (key.compareTo(treeItem.getKey()) < 0) {
				// search the left subtree
				node.setLeftChild(deleteItem(node.getLeftChild(), key));
			} else {
				// search the right subtree
				node.setRightChild(deleteItem(node.getRightChild(), key));
			} // end if
		} // end if
		return node;
	} // end deleteItem




	/**
	 * The <b>private recursive</b> method to delete the specified <b>TreeNode</b>.
	 * There are four cases to consider:
	 * <ol>
	 * <li>The node is a leaf: just remove the node.</li>
	 * <li>The node has no left child: replace the node with it's right child</li>
	 * <li>The node has no right child: replace the node with it's left child</li>
	 * <li>The node has two children: find the in-order successor, swap TreeItems and delete the successor</li>
	 * </ol>
	 * This method is initally called by the {@link #deleteItem <b><u>deleteItem</u></b>} method
	 *
	 * @param node The current TreeNode being examined for deletion
	 * @return The TreeNode reference that will replace the deleted TreeNode
	 */
//	private TreeNode<K, V> deleteNode(TreeNode<K, V> node) {
//		if ((node.getLeftChild() == null) && (node.getRightChild() == null)) {
//			// node is a leaf
//			return null;
//		} else if (node.getLeftChild() == null) {
//			// no left child
//			return node.getRightChild();
//		} else if (node.getRightChild() == null) {
//			// no right child
//			return node.getLeftChild();
//		} else {
//			// there are two children:
//			TreeNode<K, V> successorNode;
//			successorNode = findLeftmost(node.getRightChild());
//			node.setRightChild(deleteLeftmost(node.getRightChild()));
//			return node;
//		}   // end if
//	}   // end deleteNode
//



	/**
	 * The <b>private recursive</b> method to find the left-most child of a subtree
	 * This method is initially called by the {@link #deleteNode <b><u>deleteNode</u></b>} method
	 *
	 * @param node The current TreeNode being examined for being the left-most child
	 * @return The TreeNode reference that is the left-most child
	 */
	private TreeNode<K, V> findLeftmost(TreeNode<K, V> node) {
		if (node.getLeftChild() == null) {
			return node;
		} else {
			return findLeftmost(node.getLeftChild());
		}   // end if
	}   // end findLeftmost


	/**
	 * The <b>private recursive</b> method to deleted the left-most child of a subtree
	 * This method is initially called by the {@link #deleteNode <b><u>deleteNode</u></b>} method
	 *
	 * @param node The current TreeNode being examined for being the left-most child
	 * @return The TreeNode reference that is the left-most child
	 */
	private TreeNode<K,V> deleteNode( TreeNode<K,V> node)  {
		if ((node.getLeftChild() == null) && (node.getRightChild() == null)  )  {
			// node is a leaf
			return null;
		} else if (node.getLeftChild() == null) {
			// no left child
			return node.getRightChild();
		} else {
			if (node.getRightChild() == null) {
				// no right child
				return node.getLeftChild();
			} else {
				// there are two children:
				TreeNode<K, V> successorNode;
				successorNode = findLeftmost(node.getRightChild());
				node.setTreeItem(successorNode.getTreeItem());
				//TreeNode deleteLeftmostNode;
				node.setRightChild((deleteLeftmost(node.getRightChild())));
				return node;
			}   // end if
		}
	}   // end deleteNode

	private TreeNode<K,V> deleteLeftmost(TreeNode<K, V> rightChild) {

		return null;
	}

//The recursive helper method

	// The private recursive method to calculate the height of a subtree rooted at the specified TreeNode
	public int height() {

		// call the private recursive method
		return treeHeight(root);
	}


//The recursive helper method

	// The private recursive method to calculate the height of a subtree rooted at the specified TreeNode
	private int treeHeight(TreeNode<K, V> node) {


//Here, if  the node is null height is 0 (Base case of recursion.

		// empty node, means no height
		if (node == null) {
			return 0;
		}


//Otherwise, we get the left and right height. Then we return the greater one :

		else {
			// get height of left subtree (with recursion)
			int leftHeight = treeHeight(node.getLeftChild());

			// get height of right subtree (with recursion)
			int rightHeight = treeHeight(node.getRightChild());

			// return the bigger height
			// make sure to add +1 for the height of the current node
			if (leftHeight > rightHeight)
				return (leftHeight + 1);
			else
				return (rightHeight + 1);
		}
	}


//The next method is isBalanced(). Here, we also have a recursive helper.


//So the method itself is :

	// This is the method the user calls to find out if the BinarySearchTree is balanced.
	public boolean isBalanced() {
		// call the private recursive method
		return isBalancedSubtree(root);
	}


//The recursive helper method


	// The private recursive method to determine if the subtree rooted at the given node is balanced
	private boolean isBalancedSubtree(TreeNode<K, V> node) {

		if( node == null)

			return true;

//This will also need some help from the previous height methods. We start by getting left-right heights :

		// get height of left subtree (with treeHeight method)
		int leftHeight = treeHeight(node.getLeftChild());

		// get height of right subtree (with treeHeight method)
		int rightHeight = treeHeight(node.getRightChild());


//Now, the height difference :

		// obtain the left-right height difference
		int heightDifference = leftHeight - rightHeight;


//This should only be -1, 0, or 1 for balance

		// height difference can be a max of 1 (on left side or right side)
		if (heightDifference == -1 || heightDifference == 0 || heightDifference == 1) {
			// check subtree height balance on both and right child
			if (isBalancedSubtree(node.getLeftChild()) && isBalancedSubtree(node.getRightChild()))
				// when all success, return true
				return true;
		}


//And recursively we also check if left and right subtrees are balanced.


//If the conditions fail, then we return false:


		// not found, return false
		return false;
	}

	public void balance() {
		TreeIterator<K, V> it = new TreeIterator<K, V>((this));
		it.setInorder();

		int count = it.size();

		java.lang.Object[] arr = new Object[count];


		for (int i = 0; i < count; i++)
			arr[i] = it.next();
		root = balanceTree(arr, 0, count - 1);
	}

	private TreeNode<K, V> balanceTree(Object[] arr, int first, int last) {

		if (first > last)
			return null;

		int center = (first+last)/2;
		TreeItem<K,V> item = (TreeItem<K,V>)arr[center];

		//now the construct left and right subtree

		TreeNode<K,V> node = new TreeNode<K,V>(item);
		node.setLeftChild(balanceTree(arr, first, center-1));
		node.setRightChild(balanceTree(arr, center+1, last));

		return node;
	}

}