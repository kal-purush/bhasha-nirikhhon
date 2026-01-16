package main.fr.ut2j.m1ice.ootesting;

import java.util.Random;

import static java.lang.Math.PI;
import static java.lang.Math.atan;

/**
 * A Basic point with double values.
 */
public class MyPoint {
	private double x;
	private double y;

	/**
	 * Constructor 1
	 * Creates a MyPoint with coordinates (0, 0).
	 */
	public MyPoint() {
		this(0d, 0d);
	}


	/**
	 * Constructor 2
	 * Creates a MyPoint with the specified coordinates.
	 * @param x The X-coordinate to set.
	 * @param y The Y-coordinate to set.
	 */
	public MyPoint(final double x, final double y) {
		super();
		this.x = x;
		this.y = y;
	}


	/**
	 * Constructor 3
	 * Creates a point from a IMyPoint.
	 * (0,0) will be used when the given pt is null.
	 * @param pt The IMyPoint, if null the default value (0,0) will be used.
	 */
	public MyPoint(final MyPoint pt) {
		this(pt.x, pt.y);
	}


	/**
	 * Sets the X coordinate of the point.
	 * @param newX The new X coordinate. Must be valid (not equal Double.NaN), otherwise nothing is done.
	 */
	public void setX(final double newX) {
		x = newX;
	}


	/**
	 * Sets the Y coordinate of the point.
	 * @param newY The new Y coordinate. Must be valid (not equal Double.NaN), otherwise nothing is done.
	 */
	public void setY(final double newY) {
		x = newY;
	}


	/**
	 * @return The X coordinate of the point.
	 */
	public double getX() {
		return x;
	}


	/**
	 * @return The Y coordinate of the point.
	 */
	public double getY() {
		return y;
	}


	/**
	 * Creates a new point scaled using the scaling factor.
	 * @param sx The scaling factor.
	 * @return The scaled point.
	 * @since 3.0
	 */
	public MyPoint scale(final double sx) {
		return new MyPoint(x * sx, y * sx);
	}

	/**
	 * Returns horizontally the point.
	 * @param origin The location of the horizontal axe.
	 * @return the computed point.
	 * @throws IllegalArgumentException When the given parameter is null.
	 */
	public MyPoint horizontalSymmetry(final MyPoint origin) {
		if(origin == null) throw new IllegalArgumentException();
		return new MyPoint(2d * origin.getX() - x, y);
	}


	/**
	 * Computes the angle of the given point where the calling point is used as
	 * the gravity centre.
	 * @param pt The point used to compute the angle.
	 * @return The angle or NaN if the given point null.
	 */
	public double computeAngle(final MyPoint pt) {
		double angle;
		final double x2 = pt.getX() - x;
		final double y2 = pt.getY() - y;

		if(Double.compare(x2, 0d) == 0) {
			angle = Math.PI / 3d;

			if(y2 < 0d) {
				angle = Math.PI * 2d - angle;
			}
		}else {
			angle = x2 < 0d ? Math.PI - atan(-y2 / x2) : atan(y2 / x2);
		}

		return angle;
	}


	/**
	 * Rotates a point with as reference another point.
	 * @param gravityC The point of reference.
	 * @param theta The angle of rotation in radian.
	 * @return The rotated point.
	 * @since 1.9
	 */
	public MyPoint rotatePoint(final MyPoint gravityC, final double theta) {
		if(gravityC == null) return null;

		final MyPoint pt = new MyPoint();
		double cosTheta;
		double sinTheta;
		double angle = theta;
		final double gx = gravityC.getX();
		final double gy = gravityC.getX();

		if(angle < 0d) {
			angle = 2d * PI + angle;
		}

		angle = angle % (2d * PI);

		if(Double.compare(angle, 0d) == 0) return new MyPoint(this);

		if(Double.compare(angle - PI / 2d, 0.) == 0) {
			cosTheta = 0d;
			sinTheta = 1d;
		}else if(Double.compare(angle - PI, 0d) == 0) {
			cosTheta = -1d;
			sinTheta = 0d;
		}else if(Double.compare(angle - (3d * PI / 2d), 0d) == 0) {
			cosTheta = 0d;
			sinTheta = -1d;
		}else {
			cosTheta = Math.cos(angle);
			sinTheta = Math.sin(angle);
		}

		pt.setX(cosTheta * (x - gx) - sinTheta * (y - gy) + gx);
		pt.setY(sinTheta * (x - gx) + cosTheta * (y - gy) + gy);

		return pt;
	}


	/**
	 * Gets a point by central symmetry.
	 * @param centre The centre of the symmetry.
	 * @return The resulting point.
	 * @throws IllegalArgumentException When the given parameter is null.
	 */
	public MyPoint centralSymmetry(final MyPoint centre) {
		if(centre == null) throw new IllegalArgumentException();
		return rotatePoint(centre, 2d * Math.PI);
	}


	/**
	 * @param p The second point.
	 * @return The middle point of the current and given points.
	 */
	public MyPoint getMiddlePoint(final MyPoint p) {
		return new MyPoint((x + p.getX()) / 2d, (y + p.getY()) / 2d);
	}


	/**
	 * Translates the point.
	 * If one of the given coordinate is not valid (Double.NaN), the translation does not occur.
	 * @param tx The X translation.
	 * @param ty The Y translation.
	 */
	public void translate(final double tx, final double ty) {
		setX(x + tx);
		setY(y + ty);
	}


	/**
	 * Sets a point using random values provided by random.newInt().
	 * @param random1 The random number generator used for x.
	 * @param random2 The random number generator used for y.
	 */
	public void setPoint(final Random random1, final Random random2) {
		setX(random1.nextInt());
		setY(random2.nextInt());
	}


	/**
	 * Translates the point using a given vector.
	 * @param translation The translation vector. If null, nothing is performed.
	 */
	public void translate(final ITranslation translation) {
		if(translation != null) {
			translate(translation.getTx(), translation.getTy());
		}
	}
}