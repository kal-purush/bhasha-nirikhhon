package test;

import java.awt.event.MouseEvent;

import gui.elements.Arrow;
import gui.elements.Cannon;
import gui.elements.Char1;
import gui.elements.Char2;
import gui.elements.Char3;
import gui.elements.Char4;
import gui.elements.Char5;


public interface AcceptStrategy {
	
	<T> T accept(ItemsVisitors<T> visitor,MouseEvent e,Arrow staticArrow,Arrow[] arrow);
	
	<T> T accept(ItemsVisitors<T> visitor,MouseEvent e,Cannon staticCannon,Cannon[] arrow);
	
	<T> T accept(ItemsVisitors<T> visitor,MouseEvent e,Char1 staticChar1,Char1[] char1);
	
	<T> T accept(ItemsVisitors<T> visitor,MouseEvent e,Char2 staticChar2,Char2[] char2);
	
	<T> T accept(ItemsVisitors<T> visitor,MouseEvent e,Char3 staticChar3,Char3[] char3);
	
	<T> T accept(ItemsVisitors<T> visitor,MouseEvent e,Char4 staticChar4,Char4[] char4);
	
	<T> T accept(ItemsVisitors<T> visitor,MouseEvent e,Char5 staticChar5,Char5[] char5);
}