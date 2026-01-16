using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;


class Vector2Sort : IComparer<Vector3>
{
	private Vector2 center;

	public Vector2Sort(Vector3 center)
	{
		this.center = center;
	}

	private int secDetermine(Vector3 a)
	{
		if (a.x >= 0 && a.y <= 0)
			return 1;
		else if (a.x >= 0 && a.y > 0)
			return 2;
		else if (a.x < 0 && a.y > 0)
			return 3;
		else
			return 4;
	}

	public int Compare(Vector3 a, Vector3 b)
	{
		//bool less(point a, point b)

		// {

		// lets make center our origo and translate vectors to that

		a.x -= center.x;
		b.x -= center.x;
		a.y -= center.y;
		b.y -= center.y;

		a.Normalize();
		b.Normalize();


		// Sweep anticlockwise and test the angles; smaller gets drawn first
		if (Math.Atan2(a.x, a.y) < Math.Atan2(b.x, b.y))
			return 1;
		else if (Math.Atan2(a.x, a.y) > Math.Atan2(b.x, b.y))
			return -1;
		else
			return 0;
		/*
        int aSec = 4;   //secDetermine(a);
        int bSec = 4;//secDetermine(b);
        // Then we divide coordinates into four sections; positive x and negative y I, positive x positive y II etc ccw
        // Draw I first, then II etc
        if (aSec < bSec)
            return -1;
        else if (aSec > bSec)
            return 1;
        else
        // in case both are in the same quadrant
        // we have four cases with different criteria
        {
            // I 
            // draw x closest to zero first, if both same then furthest y
            if (aSec == 1)
            {
                if (a.y > b.y)
                    return -1;
                else if (a.y < b.y)
                    return 1;
                else
                {
                    if (a.x < b.x)
                        return 1;
                    if (a.x > b.x)
                        return -1;
                    else // they are actually same points
                        return 0;
                }
            }
            // II
            // draw x closest to zero first, if both same then closest y
            else if (aSec == 2)
            {
                if (a.y < b.y)
                    return -1;
                else if (a.y > b.y)
                    return 1;
                else
                {
                    if (a.x > b.x)
                        return -1;
                    if (a.x < b.x)
                        return 1;
                    else // they are actually same points
                        return 0;
                }
            }
            // III
            // draw x closest to zero first, if both same then furthest y
            else if (aSec == 3)
            {
                if (a.y < b.y)
                    return -1;
                else if (a.y > b.y)
                    return 1;
                else
                {
                    if (a.x < b.x)
                        return -1;
                    if (a.x > b.x)
                        return 1;
                    else // they are actually same points
                        return 0;
                }
            }
            // IV
            // draw x closest to zero first, if both same then closest y
            else 
            {
                if (a.x < b.x)
                    return 1;
                else if (a.x > b.x)
                    return -1;
                else
                {
                    if (a.y < b.y)
                        return 1;
                    if (a.y>  b.y)
                        return -1;
                    else // they are actually same points
                        return 0;
                }
            }
        }
        /*
        // Is the x same with center (is the points above or below center x?)
		if (a.x == 0 && b.x != a.x)
		{
			// a is on the center, b isn't. Draw a first if it's below axis and b is on the right side
			if (a.y < 0 && b.x > 0)
			{
				return -1;
			}
			else if (a.y < 0 && b.x < 0)
			{
				return 1;
			}
			// a is above, so we check if b.x is positive (on the right side)
			if (a.y > 0 && b.x > 0)
			{
				return 1;
			}
			else if (a.y > 0 && b.x < 0)
			{
				return -1;
			}
		}
		else if (b.x == 0 && b.x != a.x)
		{
			// b is on the center, a isn't. Draw b first if it's below axis and a is on the right side
			if (b.y < 0 && a.x > 0)
			{
				return -1;
			}
			else if (b.y < 0 && a.x < 0)
			{
				return 1;
			}
			// a is above, so we check if b.x is positive (on the right side)
			if (b.y > 0 && a.x > 0)
			{
				return 1;
			}
			else if (b.y > 0 && a.x < 0)
			{
				return -1;
			}
		}
		else if (a.x == b.x && a.x - center.x == 0)
		{ // okay both are
			if (a.y == b.y) // if y same then same
				return 0;
			else if (a.y < b.y) // if a lower then it goes first
				return -1;
			else
				return 1;
		}
		// is it right side or left side? remember we go counter clockwise
		if (a.x > 0 && b.x < 0)
			return -1;
		else if (b.x > 0 && a.x < 0)
			return 1;
		else
		{
			// okay both are on the same side, time to chekc which side
			if (a.x > 0) // if positive then on right side
			{
				// draw closer first
				if (a.x < b.x)
					return -1;
				else if (a.x > b.x)
					return 1;
				else
				{
					// okay again x is same so draw lowest y first
					if (a.y < b.y)
						return -1;
					else
						return 1;
				}
			}
			else // now on the left side bigger goes first
			{

				// draw bigger first
				if (a.x > b.x)
					return -1;
				else if (a.x < b.x)
					return 1;
				else
				{
					// okay again x is same so draw higest y first
					if (a.y > b.y)
						return -1;
					else
						return 1;
				}

			}
		}
		/*
    if (center.y - a.y < center.y - b.y)
        return -1;
    else if (center.y - a.y > center.y - b.y)
        return 1;
    else if (a.x == b.x && a.y == b.y)
        return 0;
    else
        return 0;
        */
		/*
		if (a.x - center.x == 0 && b.x - center.x == 0)
		{
			if (a.y - center.y >= 0 || b.y - center.y >= 0)
			if (a.y > b.y)
				return 1;
			else if (a.y < b.y)
				return -1;
		}
		// compute the cross product of vectors (center -> a) x (center -> b)
		float det = (a.x - center.x) * (b.y - center.y)
			-(b.x - center.x) * (a.y - center.y);
		if (det < 0)
			return 1;
		if (det > 0)
			return -1;
		// points a and b are on the same line from the center
		// check which point is closer to the center
		float d1 = (a.x - center.x) * (a.x - center.x) + (a.y - center.y) * (a.y - center.y);
		float d2 = (b.x - center.x) * (b.x - center.x) + (b.y - center.y) * (b.y - center.y);
		if (d1 > d2)
			return -1;
		else
			return 1;*/


	}
}