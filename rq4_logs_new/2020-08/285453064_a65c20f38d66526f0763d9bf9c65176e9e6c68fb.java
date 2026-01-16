package br.main;

import br.main.player.visual.JPlayerMp3;

/**
 * https://introcs.cs.princeton.edu/java/faq/mp3/mp3.html<br />
 * http://www.javazoom.net/javalayer/javalayer.html<br />
 * https://introcs.cs.princeton.edu/java/faq/mp3/MP3.java.html<br />
 * https://stackoverflow.com/questions/16882354/how-to-play-pause-a-mp3-file-using-the-javazoom-jlayer-library<br
 * />
 */
public class Main {

	public static void main(String[] args) {
		JPlayerMp3 jplayer = new JPlayerMp3();
		jplayer.setVisible(true);
	}

}