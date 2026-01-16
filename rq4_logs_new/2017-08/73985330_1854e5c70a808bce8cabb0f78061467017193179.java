// Description:
//
// Rebuild a tree given both the in-order and pre-order traversals.

package epi;

import java.util.Arrays;
import java.util.List;
import rlib.BinaryTreeNode;
import rlib.TreeUtils;
import static rlib.TestingUtils.expect;

public class TreeFromTraversals {

  public static BinaryTreeNode<Integer> treeFromTraversals(
    List<Integer> inorder, List<Integer> preorder)
  {
    if (inorder.size() == 0 || preorder.size() == 0) return null;
    int rootIndex = inorder.indexOf(preorder.get(0));
    if (rootIndex < 0) return null;
    List<Integer> inorderLeft = inorder.subList(0, rootIndex);
    List<Integer> inorderRight = inorder.subList(rootIndex + 1, inorder.size());
    List<Integer> preorderLeft = preorder.subList(1, rootIndex + 1);
    List<Integer> preorderRight = preorder.subList(rootIndex + 1, preorder.size());
    BinaryTreeNode<Integer> root = new BinaryTreeNode<>(inorder.get(rootIndex));
    root.left = treeFromTraversals(inorderLeft, preorderLeft);
    root.right = treeFromTraversals(inorderRight, preorderRight);
    return root;
  }

  private static void smallTest() {
    List<Integer> inorder, preorder;
    BinaryTreeNode<Integer> result, expected;

    inorder = Arrays.asList(4, 2, 5, 1, 6, 3, 7);
    preorder = Arrays.asList(1, 2, 4, 5, 3, 6, 7);
    result = treeFromTraversals(inorder, preorder);
    expected = TreeUtils.buildTree(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
    expect(TreeUtils.equivalence(result, expected)).toBe(true);

    inorder = Arrays.asList(1, 6, 3, 7);
    preorder = Arrays.asList(1, 3, 6, 7);
    result = treeFromTraversals(inorder, preorder);
    expected = TreeUtils.buildTree(Arrays.asList(1, null, 3, null, null, 6, 7));
    expect(TreeUtils.equivalence(result, expected)).toBe(true);
  }

  public static void main(String[] args) {
    smallTest();
  }

}