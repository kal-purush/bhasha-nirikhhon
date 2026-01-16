import { useEffect, useState } from "react";
import { DEFAULT_DEVICE_ORIENTATION } from "./constants";
import {
  DetectDeviceOrientation,
  Orientation,
} from "detect-device-orientation";

// Create an instance of DetectDeviceOrientation
const detectDeviceOrientation = new DetectDeviceOrientation();

/**
 * A React hook to get the device orientation.
 * It uses the DetectDeviceOrientation class to get the current device orientation.
 * The orientation is updated whenever the device orientation changes.
 * @returns {Orientation} The current device orientation.
 */
export const useDeviceOrientation = () => {
  // State to hold the current device orientation
  const [deviceOrientation, setDeviceOrientation] = useState<Orientation>(
    DEFAULT_DEVICE_ORIENTATION,
  );

  useEffect(() => {
    // Initialize the device orientation detection
    detectDeviceOrientation.init((orientation) => {
      // Update the device orientation state
      setDeviceOrientation({
        absolute: orientation.absolute,
        alpha: orientation.alpha,
        beta: orientation.beta,
        gamma: orientation.gamma,
        webkitCompassHeading: orientation.webkitCompassHeading,
        webkitCompassAccuracy: orientation.webkitCompassAccuracy,
      });
    });

    // Clean up the device orientation detection when the component unmounts
    return () => {
      detectDeviceOrientation.reset();
    };
  }, []);

  // Return the current device orientation
  return deviceOrientation;
};