/**
   AVLTree is a special binary tree that is able the maintain the height of a leaf and any node to be 1 at most

   @author Tumelo Lephadi
   @version 10 April 2017
   reference: kukuruku.co/post/avl-trees/
*/
public class AVLTree<dataType extends Comparable<? super dataType>> extends BinaryTree<dataType>
{
   /**
      @param node node that height is measured from
      @return returns height the height of the node or -1 if the tree is empty
   */
   public int height (BinaryTreeNode<dataType> node)
   {
      if (node != null)
         return node.height;
      return -1;
   }

   /**
      @param node node that height difference of children is measured from
      @return returns the differences in the height of the children of the parent node
   */
   public int balanceFactor (BinaryTreeNode<dataType> node)
   {
      return height (node.right) - height (node.left);
   }

   /**
      @param node node that will have it's height adjusted
   */
   public void fixHeight (BinaryTreeNode<dataType> node)
   {
      node.height = Math.max (height (node.left), height (node.right)) + 1;
   }

   /**
      @param p the node that will be a new parent after it is rotated
      @return returns a node that was once a child but is now a parent
   */
   public BinaryTreeNode<dataType> rotateRight (BinaryTreeNode<dataType> p)
   {
      BinaryTreeNode<dataType> q = p.left;
      p.left = q.right;
      q.right = p;
      fixHeight (p);
      fixHeight (q);
      return q;
   }

   /**
      @param q the node that will be a new parent after it is rotated
      @return returns a node that was once a child but is now a parent
   */
   public BinaryTreeNode<dataType> rotateLeft (BinaryTreeNode<dataType> q)
   {
      BinaryTreeNode<dataType> p = q.right;
      q.right = p.left;
      p.left = q;
      fixHeight (q);
      fixHeight (p);
      return p;
   }

    /**
      @param p the node that will be a new parent after it is rotated
      @return returns a node that was once a child but is now a parent
   */

   public BinaryTreeNode<dataType> balance (BinaryTreeNode<dataType> p)
   {
      fixHeight (p);
      if (balanceFactor (p) == 2)
      {
         if (balanceFactor (p.right) < 0)
            p.right = rotateRight (p.right);
         return rotateLeft (p);
      }
      if (balanceFactor (p) == -2)
      {
         if (balanceFactor (p.left) > 0)
            p.left = rotateLeft (p.left);
         return rotateRight (p);
      }
      return p;
   }

   /**
      @param d1 data in the node
      @param d2 data in the node
   */
   public void insert (dataType d1, dataType d2)
   {
      root = insert (d1, d2, root, 0);
   }

   /**
      @param d1 data in the node
      @param d2 data in the node
      @param node node that is added to the tree
      @param h height of node
      @return returns the a node that is added to the tree
   */

   public BinaryTreeNode<dataType> insert (dataType d1, dataType d2, BinaryTreeNode<dataType> node, int h)
   {
      if (node == null)
         return new BinaryTreeNode<dataType> (d1, d2, null, null, 0);
      if (d1.compareTo (node.data1) <= 0)
         node.left = insert (d1, d2, node.left, node.height);
      else
         node.right = insert (d1, d2, node.right, node.height);
      return balance (node);
   }

   /**
      @param d1 data in node to be deleted
   */
   public void delete (dataType d1)
   {
      root = delete (d1, root);
   }

   /**
      @param node node that will be deleted
      @param d1 data that is in node that will be deleted
      @return returns a tree without deleted node
   */
   public BinaryTreeNode<dataType> delete (dataType d1, BinaryTreeNode<dataType> node)
   {
      if (node == null) return null;
      if (d1.compareTo (node.data1) < 0)
         node.left = delete (d1, node.left);
      else if (d1.compareTo (node.data1) > 0)
         node.right = delete (d1, node.right);
      else
      {
         BinaryTreeNode<dataType> q = node.left;
         BinaryTreeNode<dataType> r = node.right;
         if (r == null)
            return q;
         BinaryTreeNode<dataType> min = findMin (r);
         min.right = removeMin (r);
         min.left = q;
         return balance (min);
      }
      return balance (node);
   }

   /**
      @param node a node that has the smallest data
      @return returns the node with the smallest data
   */
   public BinaryTreeNode<dataType> findMin (BinaryTreeNode<dataType> node)
   {
      if (node.left != null)
         return findMin (node.left);
      else
         return node;
   }

   /**
      @param node node that will be removed
      @return returns tree without removed node
   */
   public BinaryTreeNode<dataType> removeMin (BinaryTreeNode<dataType> node)
   {
      if (node.left == null)
         return node.right;
      node.left = removeMin (node.left);
      return balance (node);
   }

   /**
      @param d1 data that is queried in tree
      @return returns a node that has the data queried
   */
   public BinaryTreeNode<dataType> find (dataType d1)
   {
      if (root == null)
         return null;
      else
         return find (d1, root);
   }

   /**
      @param d1 data that is queried in tree
      @param node node that has data queried
      @return returns the node that has the data queried
   */
   public BinaryTreeNode<dataType> find (dataType d1, BinaryTreeNode<dataType> node)
   {
      if (d1.compareTo (node.data1) == 0)
         return node;
      else if (d1.compareTo (node.data1) < 0)
         return (node.left == null) ? null : find (d1, node.left);
      else
         return (node.right == null) ? null : find (d1, node.right);
   }

   //prints the level that a node is at
   public void treeOrder ()
   {
      treeOrder (root, 0);
   }

   /**
      @param node node that will have the order printed from
      @param level level that the method will stop printing from
   */
   public void treeOrder (BinaryTreeNode<dataType> node, int level)
   {
      if (node != null)
      {
         for ( int i=0; i<level; i++ )
            //System.out.print (" ");
         System.out.println (node.data1);
         treeOrder (node.left, level+1);
         treeOrder (node.right, level+1);
      }
   }
}