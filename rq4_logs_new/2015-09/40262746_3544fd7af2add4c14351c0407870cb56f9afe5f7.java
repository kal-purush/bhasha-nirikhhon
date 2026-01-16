package com.redstoner.nemes.t3tris.debug;

import java.util.Random;

import com.redstoner.nemes.t3tris.gameplay.Shape;

public class DebugShapeGenerator {

	public static Shape getFullLayer(Random rand) {
		return new ShapeLayer(rand);
	}
}