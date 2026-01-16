//   输入某二叉树的前序遍历和中序遍历的结果，请重建出该二叉树。假设输入的
// 前序遍历和中序遍历的结果中都不含重复的数字。例如输入前序遍历序列{1,2,4
// ,7,3,5,6,8}和中序遍历序列{4,7,2,1,5,3,8,6}，则重建二叉树并返回。


/* function TreeNode(x) {
    this.val = x;
    this.left = null;
    this.right = null;
} */
function reConstructBinaryTree(pre, vin)
{
    // write code here
	if(vin.length === 0)
        return null;
     
    var root = 0, i, j;
    var left_pre = [], right_pre = [], left_in = [], right_in = [];
     
    var head = new TreeNode(pre[0]);
    for(i = 0; i < vin.length; i++){
        if(vin[i] === pre[0]){
            root = i;
            break;
        }
    }
    for(j = 0; j < root; j++){
        left_pre.push(pre[j+1]);
        left_in.push(vin[j]);
    }
    for(j = root + 1; j < vin.length; j++){
        right_pre.push(pre[j]);
        right_in.push(vin[j]);
    }
     
    head.left = reConstructBinaryTree(left_pre, left_in);
    head.right = reConstructBinaryTree(right_pre, right_in);
     
    return head;
}