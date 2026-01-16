package threads;

import java.util.Queue;

import algorithms.Graph_Algo;
import dataStructure.game_metadata;
import dataStructure.node_data;
import items.Fruit;
import items.Robot;

public class manageRobot extends Thread {
	
	
	private game_metadata game_mt;
	private Robot robot;


	public manageRobot(game_metadata game_mt, Robot robot) {
		this.game_mt = game_mt;
		this.robot = robot;
	}
	
	
	
	public void run() {
		System.out.println("Thread #" + getId() + " started");
		
		while (game_mt.service.isRunning()) {
			
			try {
				sleep(500);
				
				
				if (robot.isBusy()) {
					moveMe();
				}
				else {
					System.out.println(robot.getID() + "; " + robot.getDest());
					game_mt.controller.allocateFruitToRobot(robot);
				}
				
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} // end while
	}

	private void moveMe() {
		
		System.out.println("fruits graph:");
		for (int j = 0; j < game_mt.getFruits().size(); j++) {
			System.out.print("(" +game_mt.getFruits().get(j).getPos().x() + "," + game_mt.getFruits().get(j).getPos().y() +") ");
		}
		System.out.println();
		

			System.out.println("robot 0, src: " + robot.getSrc() + ", DEST:" + robot.getDest());
			if (!robot.getPath_to_target().isEmpty() && robot.getDest() == -1) {
				int go_where = robot.getPath_to_target().poll().getKey();
				System.out.println("popped " + go_where);
				System.out.println("ROBOT " + robot.getID() + ", going to: " + go_where);
				robot.setDest(go_where);
				System.out.println("GOING TO " + go_where);
				game_mt.service.chooseNextEdge(robot.getID(), go_where); // request the robot to move
				//System.out.println("ROBOT: " + current_robot.getSrc() + "," + current_robot.getSrc());

			}
			
			if (robot.getPath_to_target().size() == 0) {robot.setIsBusy(false); System.out.println("I am Free");}
		//	if (robot.getPath_to_target().isEmpty()) { // if the robot reached its destination then remove it from busy_robots and add it to available robots
		//		robot.setIsBusy(false);
		//		}
		

	}
}