package org.jal.collections.heap;

class ArrayTree {
  public static int getParent(int index, int root) {
    assert root >= 0;

    return getParent(index-root) + root;
  }

  public static int getLeftChild(int index, int root) {
    assert root >= 0;

    return getLeftChild(index-root) + root;
  }

  public static int getRightChild(int index, int root) {
    assert root >= 0;

    return getRightChild(index-root) + root;
  }

  public static int getLastInternalNode(int last, int root) {
    assert root >= 0;

    return getLastInternalNode(last-root) + root;
  }

  private static int getParent(int index) { // for root zero
    assert index >= 1;

    return (index-1)/2;
  }

  private static int getLeftChild(int index) { // for root zero
    assert index >= 0;

    return 2*index+1;
  }

  private static int getRightChild(int index) { // for root zero
    assert index >= 0;

    return 2*index+2;
  }

  private static int getLastInternalNode(int last) { // for root zero
    assert last >= 1;

    return (last-1)/2;
  }
}