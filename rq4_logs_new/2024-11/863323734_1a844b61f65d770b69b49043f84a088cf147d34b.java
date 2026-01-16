class Solution {
    public char[][] rotateTheBox(char[][] box) {
        int m = box.length; // Number of rows
        int n = box[0].length; // Number of columns
        
        // Simulate gravity in the box
        for (int i = 0; i < m; i++) {
            int emptySlot = n - 1; // Start from the rightmost position
            for (int j = n - 1; j >= 0; j--) {
                if (box[i][j] == '*') {
                    // Reset emptySlot to the left of the obstacle
                    emptySlot = j - 1;
                } else if (box[i][j] == '#') {
                    // Move stone to the rightmost available slot
                    box[i][j] = '.';
                    box[i][emptySlot] = '#';
                    emptySlot--;
                }
            }
        }

        // Rotate the box 90 degrees clockwise
        char[][] rotatedBox = new char[n][m];
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                rotatedBox[j][m - 1 - i] = box[i][j];
            }
        }

        return rotatedBox;
    }
}