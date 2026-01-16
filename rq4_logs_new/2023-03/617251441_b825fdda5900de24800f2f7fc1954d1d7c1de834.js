
/**
 * Binary Tree (二叉树)
 * 
 * 二叉树 计算每层最多有个几个元素
 * 
 * Level 0: 2^0 = 1;
 * Level 1: 2^1 = 2;
 * Level 2: 2^2 = 4;
 * Level 3: 2^3 = 8;
 * ....
 * Level N: nodes = 2^n;
 * Tree Total Nodes = 2^H-1 (h 是树的深度)
 * 
 * Binary Search Tree Big O 探索
 * 假设一颗树有100个元素，高为h，则 2^h=100, 因此： 
 * lookup O(log N)
 * insert O(log N)
 * delete O(log N)
 * 二叉搜索树非常适合比较，比 hash table 更好。
 * 
 * 什么是平衡二叉查找树？为什么说不平衡的搜索树是坏的？
 * 想象一个当一个二叉树总是往左边添加元素，那么就会变成链表，CRUD 效率变成 O(n)
 * 
 * 
 */


class Node {
    constructor(value) {
        this.left = null
        this.right = null
        this.value = value
    }
}

class BinarySearchTree {
    constructor(){
        this.root = null
    }

    insert(value) {
        
    }
}