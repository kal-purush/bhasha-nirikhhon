package com.niki.algorithm;

/**
 * Created by IntelliJ IDEA.
 * User: niki.yang
 * Date: 2020/11/9
 */
public class AvlTree< T extends Comparable< ? super T>> {

    private static class AvlNode< T>{//avl树节点
        AvlNode( T theElement ) {
            this( theElement, null, null );
        }
        AvlNode( T theElement, AvlNode< T> lt, AvlNode< T> rt ) {
            element  = theElement;
            left     = lt;
            right    = rt;
            height   = 0;
        }
        T           element;      // 节点中的数据
        AvlNode<T>  left;         // 左儿子
        AvlNode<T>  right;        // 右儿子
        int         height;       // 节点的高度
    }

    private AvlNode< T> insert( T x, AvlNode< T> t ) {
        if( t == null ) {
            return new AvlNode< T>( x, null, null );
        }

        int compareResult = x.compareTo( t.element );

        if( compareResult < 0 ) {
            t.left = insert(x, t.left);//将x插入左子树中
            if (height(t.left) - height(t.right) == 2){ //打破平衡
                if (x.compareTo(t.left.element) < 0) { //LL型（左左型）
                    t = rotateWithLeftChild(t);
                } else {  //LR型（左右型）
                    t = doubleWithLeftChild(t);
                }
            }
        } else if( compareResult > 0 ) {
            t.right = insert( x, t.right );//将x插入右子树中
            if( height( t.right ) - height( t.left ) == 2 ) {//打破平衡
                if (x.compareTo(t.right.element) > 0) {//RR型（右右型）
                    t = rotateWithRightChild(t);
                } else {                         //RL型
                    t = doubleWithRightChild(t);
                }
            }
        } else {

        } // 重复数据，什么也不做
        t.height = Math.max( height( t.left ), height( t.right ) ) + 1;//更新高度
        return t;
    }

    //求高度
    private int height( AvlNode< T> t ) {
        return t == null ? -1 : t.height;
    }
    //带左子树旋转,适用于LL型
    private AvlNode< T> rotateWithLeftChild( AvlNode< T> k2 ) {
        AvlNode< T> k1 = k2.left;
        k2.left = k1.right;
        k1.right = k2;
        k2.height = Math.max( height( k2.left ), height( k2.right ) ) + 1;
        k1.height = Math.max( height( k1.left ), k2.height ) + 1;
        return k1;
    }

    //带右子树旋转，适用于RR型
    private AvlNode< T> rotateWithRightChild( AvlNode< T> k1 ) {
        AvlNode< T> k2 = k1.right;
        k1.right = k2.left;
        k2.left = k1;
        k1.height = Math.max( height( k1.left ), height( k1.right ) ) + 1;
        k2.height = Math.max( height( k2.right ), k1.height ) + 1;
        return k2;
    }

    //双旋转，适用于LR型
    private AvlNode< T> doubleWithLeftChild( AvlNode< T> k3 ) {
        k3.left = rotateWithRightChild( k3.left );
        return rotateWithLeftChild( k3 );
    }
    //双旋转,适用于RL型
    private AvlNode< T> doubleWithRightChild( AvlNode< T> k1 ) {
        k1.right = rotateWithLeftChild( k1.right );
        return rotateWithRightChild( k1 );
    }

    //搜索（查找）
    private boolean search( T x, AvlNode t ) {
        while( t != null ) {
            int compareResult = x.compareTo( (T) t.element );
            if( compareResult < 0 ){
                t = t.left;
            } else if( compareResult > 0 ){
                t = t.right;
            } else{
                return true;    // Match
            }
        }
        return false;   // No match
    }
}