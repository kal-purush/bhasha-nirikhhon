package com.chess.Chess;

import java.util.ArrayList;
import java.util.Objects;

public class Pawn extends Figure {
    private boolean en_passant;

    // Important for en passant!
    // After pawn moved for two squares his en_passant = true
    // So because en passant is available only for one move
    // So after every white move en_passant for every black pawn must be false
    // And after every black move en_passant for every white pawn must be false

    public Pawn(int row, int column, boolean is_white, boolean not_moved) {
        try {
            if (row >= 1 & row < 7 & column >= 0 & column < 8) {
                this.set_coordinates(row, column);
            } else {
                throw new IncorrectPositionOfFigure();
            }
        } catch (IncorrectPositionOfFigure e) {
            System.out.println(e.getMessage());
        }
        this.set_is_white(is_white);
        this.set_name("Pawn");
        this.set_not_moved(not_moved);
        this.set_en_passant(false);
    }

    public void set_en_passant(boolean en_passant) {
        this.en_passant = en_passant;
    }

    public boolean en_passant() {
        return this.en_passant;
    }

    @Override
    public boolean check_if_move_possible(Figure[][] curr_position, int new_row, int new_column) {
        int row = this.return_coordinates()[0];
        int column = this.return_coordinates()[1];
        boolean colour = this.is_white();
        try {
            if (colour) {
                if (new_row > 7 | new_row < 2 | new_column > 7 | new_column < 0) {
                    throw new ImpossibleMove();
                }
                if (new_column == column) {
                    if (new_row - row == 1) {
                        if (curr_position[new_row][column] != null) {
                            throw new ImpossibleMove();
                        }
                    } else if (new_row - row == 2) {
                        if (this.not_moved()) {
                            if (curr_position[new_row][column] != null | curr_position[row + 1][column] != null) {
                                throw new ImpossibleMove();
                            }
                        } else {
                            throw new ImpossibleMove();
                        }
                    } else {
                        throw new ImpossibleMove();
                    }
                } else if (new_column - column == 1 | new_column - column == -1) {
                    if (new_row - row != 1) {
                        throw new ImpossibleMove();
                    } else {
                        if (curr_position[new_row][new_column] == null) {
                            return this.check_for_en_passant(curr_position, new_row, new_column);
                        } else {
                            if (colour == curr_position[new_row][new_column].is_white()) {
                                throw new ImpossibleMove();
                            }
                        }
                    }
                } else {
                    throw new ImpossibleMove();
                }
            } else {
                if (new_row > 5 | new_row < 0 | new_column > 7 | new_column < 0) {
                    throw new ImpossibleMove();
                }
                if (new_column == column) {
                    if (new_row - row == -1) {
                        if (curr_position[new_row][column] != null) {
                            throw new ImpossibleMove();
                        }
                    } else if (new_row - row == -2) {
                        if (this.not_moved()) {
                            if (curr_position[new_row][column] != null | curr_position[row - 1][column] != null) {
                                throw new ImpossibleMove();
                            }
                        } else {
                            throw new ImpossibleMove();
                        }
                    } else {
                        throw new ImpossibleMove();
                    }
                } else if (new_column - column == 1 | new_column - column == -1) {
                    if (new_row - row != -1) {
                        throw new ImpossibleMove();
                    } else {
                        if (curr_position[new_row][new_column] == null) {
                            return this.check_for_en_passant(curr_position, new_row, new_column);
                        } else {
                            if (colour == curr_position[new_row][new_column].is_white()) {
                                throw new ImpossibleMove();
                            }
                        }
                    }
                } else {
                    throw new ImpossibleMove();
                }
            }
            return true;
        } catch (ImpossibleMove e) {
            return false;
        }
    }

    public boolean check_for_en_passant(Figure[][] curr_position, int new_row, int new_column) {
        int row = this.return_coordinates()[0];
        int column = this.return_coordinates()[1];
        boolean colour = this.is_white();
        if (curr_position[new_row][new_column] != null | column == new_column) {
            return false;
        }
        if (curr_position[row][new_column] != null) {
            if (Objects.equals(curr_position[row][new_column].getClass().getName(), "com.chess.Chess.Pawn")) {
                if (!(((Pawn) curr_position[row][new_column]).en_passant()) | colour == curr_position[row][new_column].is_white()) {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    @Override
    public void move(Figure[][] curr_position, int new_row, int new_column) {
        if (this.check_if_move_possible(curr_position, new_row, new_column)) {
            int row = this.return_coordinates()[0];
            int column = this.return_coordinates()[1];
            if (new_row == 0 | new_row == 7) {
                // There must be option to choose figure for promote
                this.promote(curr_position, new_row, new_column, "Queen");
            } else {
                if (this.check_for_en_passant(curr_position, new_row, new_column)) {
                    curr_position[row][new_column] = null;
                } else if (new_row - row == 2 | new_row - row == -2) {
                    ((Pawn) curr_position[row][column]).set_en_passant(true);
                }
                curr_position[new_row][new_column] = this;
                curr_position[row][column] = null;
                curr_position[new_row][new_column].set_coordinates(new_row, new_column);
                curr_position[new_row][new_column].set_not_moved(false);
            }
        } else {
            try {
                throw new ImpossibleMove();
            } catch (ImpossibleMove e) {
            }
        }
    }

    public void promote(Figure[][] curr_position, int new_row, int new_column, String name) {
        int row = this.return_coordinates()[0];
        int column = this.return_coordinates()[1];
        boolean colour = this.is_white();
        if (Objects.equals(name, "Queen")) {
            curr_position[new_row][new_column] = new Queen(new_row, new_column, colour);
        } else if (Objects.equals(name, "Rook")) {
            curr_position[new_row][new_column] = new Rook(new_row, new_column, colour, false);
        } else if (Objects.equals(name, "Bishop")) {
            curr_position[new_row][new_column] = new Bishop(new_row, new_column, colour);
        } else if (Objects.equals(name, "Knight")) {
            curr_position[new_row][new_column] = new Knight(new_row, new_column, colour, false);
        } else {
            try {
                throw new ImpossibleMove();
            } catch (ImpossibleMove e) {
            }
        }
        curr_position[row][column] = null;
    }

    @Override
    public ArrayList<int[]> get_possible_moves(Figure[][] curr_position) {
        ArrayList<int[]> possible_moves = new ArrayList<>();
        int row = this.return_coordinates()[0];
        int column = this.return_coordinates()[1];
        int[][] potential_moves = { { row + 1, column }, { row + 2, column }, { row - 1, column }, { row - 2, column },
                { row + 1, column + 1 }, { row + 1, column - 1 }, { row - 1, column + 1 }, { row - 1, column - 1 } };
        for (int[] coords : potential_moves) {
            if (this.check_if_move_possible(curr_position, coords[0], coords[1])) {
                possible_moves.add(new int[] { coords[0], coords[1] });
            }
        }
        return possible_moves;
    }
}