package problem968;

import common.TreeNode;

import static problem968.BinaryTreeCameras.NodeState.*;

public final class BinaryTreeCameras {

    enum NodeState {
        NOT_OBSERVED,
        OBSERVED,
        CAMERA_IN_PLACE
    }

    public int minCameraCover(final TreeNode root) {
        final MutableResult res = new MutableResult();
        return dfs(root, res) == NOT_OBSERVED ? res.res + 1 : res.res;
    }

    private NodeState dfs(final TreeNode root, final MutableResult result) {
        if (root == null)
            return OBSERVED;

        final NodeState leftState = dfs(root.left, result);
        final NodeState rightState = dfs(root.right, result);

        if (leftState == NOT_OBSERVED || rightState == NOT_OBSERVED) {
            result.res++;
            return CAMERA_IN_PLACE;
        }

        if (leftState == CAMERA_IN_PLACE || rightState == CAMERA_IN_PLACE)
            return OBSERVED;

        return NOT_OBSERVED;
    }

    private static final class MutableResult {
        private int res = 0;
    }

}