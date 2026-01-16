package com.minesweeper.api.domain;

import java.awt.Point;

import com.minesweeper.api.domain.enume.LockerType;

public class LockerRequest {
	
	private LockerType lockerType;
	private Point point;
	
	
	public LockerRequest(LockerType lockerType, int x, int y) {
		this.lockerType = lockerType;
		this.point = new Point(x, y);
	}
	
	public LockerType getLockerType() {
		return lockerType;
	}
	public void setLockerType(LockerType lockerType) {
		this.lockerType = lockerType;
	}
	public Point getPoint() {
		return point;
	}
	public void setPoint(Point point) {
		this.point = point;
	}

}