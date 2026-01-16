class Solution(object):
    def searchMatrix(self, matrix, target):
        """
        :type matrix: List[List[int]]
        :type target: int
        :rtype: bool
        """
        i = 0
        while i < len(matrix) - 1:
            if target > matrix[i][-1]:
                i += 1
            else:
                break
        for j in range(len(matrix[i])):
            if target == matrix[i][j]:
                return True
        return False