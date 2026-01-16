package sword_to_offer;

/**
 * P194序列化（和反序列化）二叉树,注意二叉树节点中的值不一定全是0-9的个位数，还可能有几十，几百等等
 */
public class Problem37 {

    private int index = 0;

    String Serialize(TreeNode root) {
        if (root == null){
            return "#";
        }
        return root.val + "!" + Serialize(root.left) + "!" + Serialize(root.right);
    }

    TreeNode Deserialize(String str) {
        if (str == null || str.length() == 0){
            return null;
        }
        String[] splitStr = str.split("!");
        return DeserializeTreeFromString(splitStr);
    }

    private TreeNode DeserializeTreeFromString(String[] strArray){
//        if (index >= strArray.length){  // 没有必要，因为最后一个肯定是"#"，会返回null的，不会到达长度
//            return null;
//        }
        String singleNodeValue = strArray[index++];
        if (singleNodeValue.equals("#")){
            return null;
        }
        TreeNode root = new TreeNode(Integer.parseInt(singleNodeValue));
        root.left = DeserializeTreeFromString(strArray);
        root.right = DeserializeTreeFromString(strArray);
        return root;
    }

    public static void main(String[] args){
        TreeNode root1 = new TreeNode(1);
        TreeNode root2 = new TreeNode(2);
        TreeNode root3 = new TreeNode(3);
        TreeNode root4 = new TreeNode(4);
        TreeNode root5 = new TreeNode(5);
        TreeNode root6 = new TreeNode(6);
        root1.left = root2;
        root1.right = root3;
        root2.left = root4;
        root3.left = root5;
        root3.right = root6;
        Problem37 problem37 = new Problem37();
        String treeStr = problem37.Serialize(root1);
        TreeNode root = problem37.Deserialize(treeStr);
        System.out.println("Finish Serialization");
    }
}