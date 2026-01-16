package assign1;

public class QuixoBoard {

    public static enum Face {O, X, BLANK};
    public static enum Direction {RIGHT, LEFT, UP, DOWN};
    private Face[][] _faces;
    private Face _faceOfNextMove = Face.X;

    public QuixoBoard() {
        _faces = new Face[5][5];

        for (int i = 0; i <= 4; i++)
            for (int j = 0; j <= 4; ++j)
                _faces[i][j] = Face.BLANK;
    }

    public Face faceOf(int row, int col) {
        return _faces[row][col];
    }

    public boolean validatePositionOfACubeChoosen(int row, int col) {
        if ((_faces[row][col] != _faceOfNextMove && _faces[row][col] != Face.BLANK)
                || (row > 0 && row < 4 && col > 0 && col < 4)) {
            return false;
        }

        return true;

    }

    public boolean validateMove(int row, int col, Direction direction) {

        if (!validatePositionOfACubeChoosen(row, col))
            return false;

        return (!((row == 0 && direction == Direction.UP)
                || (row == 4 && direction == Direction.DOWN)
                || (col == 0 && direction == Direction.LEFT)
                || (col == 4 && direction == Direction.RIGHT)));
    }

    public boolean move(int row, int col, Direction direction) {
        if (!validateMove(row, col, direction))
            return false;

        switch (direction) {

            case RIGHT:
                for (int j = col; j < 4; j++)
                    _faces[row][j] = faceOf(row, j + 1);

                _faces[row][4] = _faceOfNextMove;
                break;

            case LEFT:
                for (int j = col; j > 0; j--)
                    _faces[row][j] = faceOf(row, j - 1);

                _faces[row][0] = _faceOfNextMove;
                break;

            case UP:
                for (int i = row; i > 0; i--)
                    _faces[i][col] = faceOf(i - 1, col);

                _faces[0][col] = _faceOfNextMove;
                break;

            case DOWN:
                for (int i = row; i < 4; i++)
                    _faces[i][col] = faceOf(i + 1, col);

                _faces[4][col] = _faceOfNextMove;
                break;
        }

        _faceOfNextMove = (_faceOfNextMove == Face.X) ? Face.O : Face.X;
        return true;
    }

    public Face GameWinCheck() {
        Face win = Face.BLANK ;
        boolean x = (checkColumnMatch()[0] != null || checkRowMatch()[0] != null
            || checkDiagonalMatch()[0] != null);
        boolean O = (checkColumnMatch()[1] != null || checkRowMatch()[1] != null
            || checkDiagonalMatch()[1] != null);

        if (x==true && O==false)
            win = Face.X;
        if (x==false && O == true)
            win = Face.O;
        if (x==true && O == true)
            win = (_faceOfNextMove == Face.O) ? Face.O : Face.X;

        return win;
    }

    private Face[] checkRowMatch() {
        Face[] result = new Face[2];
        for (int i = 0; i < 5; i++) {
            if (_faces[i][0] == _faces[i][1]
                    && _faces[i][0] == _faces[i][2]
                    && _faces[i][0] == _faces[i][3]
                    && _faces[i][0] == _faces[i][4]
                    && _faces[i][0] != Face.BLANK) {

                if (_faces[i][0] == Face.X) {
                    result[0] = Face.X;
                } else {
                    result[1] = _faces[i][0];
                }
            }
        }
        return result;
    }

    private Face[] checkColumnMatch() {
        Face[] result = new Face[2];
        for (int i = 0; i < 5; i++) {
               if (_faces[0][i] == _faces[1][i]
                    && _faces[0][i] == _faces[2][i]
                    && _faces[0][i] == _faces[3][i]
                    && _faces[0][i] == _faces[4][i]
                    && _faces[0][i] != Face.BLANK) {
                if (_faces[0][i] == Face.X)
                    result[0] = _faces[0][i];
                else
                    result[1] = _faces[0][i];
            }
        }
        return result;
    }

    private Face[] checkDiagonalMatch() {
        Face[] result = new Face[2];

        if (_faces[0][0] == _faces[1][1]
                && _faces[0][0] == _faces[2][2]
                && _faces[0][0] == _faces[3][3]
                && _faces[0][0] == _faces[4][4]
                && _faces[0][0] != Face.BLANK) {
            if (_faces[0][0] == Face.X)
                result[0] = _faces[0][0];
            else
                result[1] = _faces[0][0];
            }

        if (_faces[0][4] == _faces[1][3]
                && _faces[0][4] == _faces[2][2]
                && _faces[0][4] == _faces[3][1]
                && _faces[0][4] == _faces[4][0]
                && _faces[0][4] != Face.BLANK) {
            if (_faces[0][4] == Face.X)
                result[0] = _faces[0][4];
            else
                result[1] = _faces[0][4];
        }

        return result;
    }

}