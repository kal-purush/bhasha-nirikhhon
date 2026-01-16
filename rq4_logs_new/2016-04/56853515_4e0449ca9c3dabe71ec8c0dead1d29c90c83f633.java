package me.spencer.phc.core;

import java.awt.KeyEventDispatcher;
import java.awt.KeyboardFocusManager;
import java.awt.event.KeyEvent;

import com.phidgets.PhidgetException; //Fuck this Exception.

import me.spencer.phc.core.controllers.InterfaceController;
import me.spencer.phc.core.controllers.MotorController;
import me.spencer.phc.core.task.TaskManager;
import me.spencer.phc.core.task.tasks.DistanceTask;

public class Main {
	
	private static InterfaceController interfaceController;
	private static MotorController motorController;
	private static TaskManager taskManager;
	
	public static void main(String[] args) throws NumberFormatException, PhidgetException, InterruptedException {
		if (args.length >= 2) {
			interfaceController = new InterfaceController(Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2]));
			motorController = new MotorController(Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2]));
			if (args.length == 3) {
				taskManager = new TaskManager();
				taskManager.addTask(new DistanceTask());
				taskManager.start();
			}
		}else{
			System.out.println("Usage: java -jar <jar> <serial> <ip> <port> [task]");
			Thread.sleep(10000L);
			System.exit(0);
		}
		
		interfaceController.openDevice();
		motorController.openDevice();
		System.out.println("Devices initialized...");
		
		KeyboardFocusManager.getCurrentKeyboardFocusManager().addKeyEventDispatcher(new KeyEventDispatcher() {
			
			@Override
			public boolean dispatchKeyEvent(KeyEvent e) {
				switch (KeyEvent.KEY_PRESSED) {
				case 87:
					try {
						motorController.moveForwardBackward(20.0D);
					} catch (PhidgetException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					break;
						
				case 83:
					try {
						motorController.moveForwardBackward(-20.0D);
					} catch (Exception e2) {
						e2.printStackTrace();
					}
					break;
					
				case 65:
					try {
						motorController.turnLeft(20.0D);
					} catch (Exception e3) {
						e3.printStackTrace();
					}
					break;
				case 68:
					try {
						motorController.turnRight(20.0D);
					} catch (PhidgetException e2) {
						e2.printStackTrace();
					}
					break;
				default:
					try {
						motorController.stop();
					} catch (PhidgetException e1) {
						e1.printStackTrace();
					}
					break;
				}
			
				return false;
			}
		});
		
	}

}