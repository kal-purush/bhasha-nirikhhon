import java.awt.Color;
import java.awt.Graphics2D;
import java.util.ArrayList;

public class NewSong {

	private ArrayList<Chord> chords;
	private long startTime;
	private long estimatedTime;
	
	public NewSong() {
		// super("Here Comes The Sun");
		chords = new ArrayList<>();
		writeSong();
		startTime = System.currentTimeMillis();
		estimatedTime = System.currentTimeMillis() - startTime;
	}

	public void writeSong() {
		chords.add(new Chord(new MusicNote(Color.GREEN, 1, "A"), new MusicNote(Color.RED, 1, "A"), 5));
		chords.add(new Chord(new MusicNote(Color.BLUE, 1, "A"), new MusicNote(Color.YELLOW, 1, "A"), 33.75));
		chords.add(new Chord(new MusicNote(Color.BLUE, 1, "A"), new MusicNote(Color.YELLOW, 1, "A"), 62.5));
		chords.add(new Chord(new MusicNote(Color.BLUE, 1, "A"), new MusicNote(Color.YELLOW, 1, "A"), 91.25));
		chords.add(new Chord(new MusicNote(Color.BLUE, 1, "A"), new MusicNote(Color.YELLOW, 1, "A"), 120));
	}	

	public void render(Graphics2D g) {
		for(int i = 0; i < chords.size(); i++) {
			Chord chord = chords.get(i);
			if(estimatedTime >= chord.getTimeToStart()) {
				chord.render(g);
			}
			System.out.println(estimatedTime);
			estimatedTime++;
		}
	}
}