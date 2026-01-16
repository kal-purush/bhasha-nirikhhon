using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public static class ExerciseSettings {

	// public data holding class
	// How-to-use:
	// Before you take out any data, be sure to check the bools
	// 'durationIsDistance' and 'trainingIsInterval' BEFORE
	// accessing data as it may return NULL

	// If 'durationIsDistance' is true, the length of the map
	// is determined by 'distance' (float of kilometers)

	// If durationIsDistance is false, the length of the map
	// is determined by a combination of 'time' (float of minutes)
	// and 'targetSpeed' (float of km/h) 
	// length of track in km = (time / 60) * targetSpeed

	public static bool durationIsDistance;
	// if durationIsDistance is true use:
	public static float distance;
	// else use:
	public static float time;
	public static float targetSpeed;


	// resistance can be accessed without any trouble
	public static float maxResistance;
	public static float minResistance;


	// If 'trainingIsInterval' is true
	// the ratio of base:peak is determined by the dropdownValue as follow:
	// 0 = 3:1 (easiest)
	// 1 = 2:1
	// 2 = 1:1
	// 3 = 1:2
	// 4 = 1:3 (hardest)
	// The unitDistance will also be available (float of km)

	// If 'trainingIsInterval' is false
	// access the climbingPercent (int of % distance spent climbing)
	// and the peakPercent (int of % distance spent at peak resistance)
	// climbingPercent and peakPercent will always add to 100

	public static bool trainingIsInterval;
	// if trainingIsInterval is true use:
	public static int basePeakDropdownValue; // 0 = 3:1, 1 = 2:1, ..., 4 = 1:3
	public static float unitDistance;
	// else use:
	public static int climbingPercent;
	public static int peakPercent;

}