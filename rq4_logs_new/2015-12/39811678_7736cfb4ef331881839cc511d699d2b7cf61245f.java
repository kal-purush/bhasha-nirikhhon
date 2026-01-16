import java.util.ArrayList;

/**
 * Created by zyt on 15/12/29 10:21.
 */
public class Offer24 {
    public class TreeNode {
        int val = 0;
        TreeNode left = null;
        TreeNode right = null;

        public TreeNode(int val) {
            this.val = val;

        }
    }

    ArrayList<ArrayList<Integer>> paths = new ArrayList<>();

    public ArrayList<ArrayList<Integer>> findPath(TreeNode root, int target) {
        if (root != null) {
            findPath(root, target, new ArrayList<Integer>());
        }
        return paths;
    }

    public void findPath(TreeNode root, int target, ArrayList<Integer> path) {
        path.add(root.val);
        int rest = target - root.val;
        if (rest > 0) {
            if (root.left != null) {
                findPath(root.left, rest, path);
            }
            if (root.right != null) {
                findPath(root.right, rest, path);
            }
        } else if (rest == 0 && root.left == null && root.right == null) {
            paths.add(new ArrayList<>(path)); //must be a new array!
        }
        path.remove(path.size() - 1);
    }
}