public class Chess {
    final char[] COLORS = new char[] {'w', 'b'};

    private Draw draw;
    private Player[] players;
    private Turn turn;
    private static final int NUM_PLAYERS = 2;

    public Chess(){
        players = new Player[NUM_PLAYERS];
        for(int i = 0; i < NUM_PLAYERS; i++){
            players[i] = new Player(COLORS[i]);
        }
        draw = new Draw(players);
        turn = new Turn();
    }

    private void play() {
        draw.show();
        do {
            players[turn.active()].movePiece();
            draw.show();
            turn.change();
        } while (!draw.checkmate());
        players[turn.noActive()].win();
    }
    public static void main(String[] args){
        new Chess().play();
    }
}