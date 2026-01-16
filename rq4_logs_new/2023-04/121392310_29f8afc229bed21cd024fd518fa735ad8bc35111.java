package leetcode.leetcode.editor.cn;

public class Main {

    public static void main(String[] args) {
        ListNode node = new ListNode(1);
        node.next = new ListNode(2);
        node.next.next = new ListNode(3);
        node.next.next.next = new ListNode(4);
        Main sol = new Main();
        int[][] input = new int[][]{{1, 6, 11},{2, 7, 12},{3, 8, 13},{4, 9, 14},{5, 10, 15}};
        int[] out = sol.solv(input, 5);
        System.out.println("Nice");
    }

    public int[] solv(int[][] input, int k) {
        int n = input[0].length;
        int l = 0, r = k-1;
        return merge(input, l, r);
    }
    public int[] merge(int[][] input, int l, int r) {
        if (l == r) {
            return input[l];
        }
        int m = (l+r) / 2;
        input[l] = mergeTwo(merge(input, l, m), merge(input, m+1, r));
        return input[l];
    }
    public int[] mergeTwo(int[] a, int[] b) {
        int ptr1 = 0, ptr2 = 0;
        int n = a.length, n2 = b.length;
        int[] output = new int[n+n2];
        int ptr = 0;
        while (ptr1 < n && ptr2 < n2) {
            if (a[ptr1] < b[ptr2]) {
                output[ptr++] = a[ptr1++];
            } else {
                output[ptr++] = b[ptr2++];
            }
        }
        while (ptr1 < n) {
            output[ptr++] = a[ptr1++];
        }
        while (ptr2 < n2) {
            output[ptr++] = b[ptr2++];
        }
        return output;
    }

}
class ListNode {
    ListNode next;
    int val;
    public ListNode (int val) {
        this.val = val;
    }
}