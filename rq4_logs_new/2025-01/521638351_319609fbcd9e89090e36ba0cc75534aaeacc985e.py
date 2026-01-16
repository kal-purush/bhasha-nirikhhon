# Definition for a binary tree node.
from typing import Optional
from collections import deque

class TreeNode:
    def __init__(self, val=0, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

class Solution:
    def widthOfBinaryTree(self, root: Optional[TreeNode]) -> int:
        q = deque([(root, 0)])
        maxWidth = 1

        while q:
            start = q[0][1]
            end = q[-1][1]
            maxWidth = max(maxWidth, end - start + 1)

            for _ in range(len(q)):
                node, pos = q.popleft()
                if node.left:
                    q.append([node.left, 2 * pos])
                if node.right:
                    q.append([node.right, 2 * pos + 1])

        return maxWidth

# TC: O(N)
# SC: O(N)