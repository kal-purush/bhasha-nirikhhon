package app;

import java.io.File;
//import java.util.List;

import model.Game;
import view.Gui;

public class App {
	
	public static void main(String[] args) {
	
		File file = new File("1");
		
		if(file != null) {
			String niveau =  file.getAbsolutePath();
			Game game = new Game(niveau);
			game.getLemmings().register(new Gui(game));
			/*game.getLemmings().register(new LemmingsObserver() {			
				@Override
				public void notify(List<LemmingsEvent> events) {				
					for (LemmingsEvent event : events) {
						System.out.println(event);
					}
				}
			});*/

			while(game.getLemmings().isAlive()) {
				game.step();
			}
		}
	}
}