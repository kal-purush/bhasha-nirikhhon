/**
 * @file Transform data test suite
 * @author Paul Brachmann
 * @license Copyright (c) 2017 Malpaux IoT All Rights Reserved.
 */

import Pointer from "../pointer";
import TransformData from "./transformData";

describe("TransformData", () => {
  it("should create a new set of transform data", () => {
    const transformData = new TransformData();
    expect(transformData.translateX).toBe(0);
    expect(transformData.translateY).toBe(0);
    expect(transformData.scale).toBe(1);
    expect(transformData.rotate2d).toBe(0);

    const transformData2 = new TransformData(128, 0, 2, 45);
    expect(transformData2.translateX).toBe(128);
    expect(transformData2.translateY).toBe(0);
    expect(transformData2.scale).toBe(2);
    expect(transformData2.rotate2d).toBe(45);

    const transformData3 = new TransformData(0, 0, 0, 0);
    expect(transformData3.translateX).toBe(0);
    expect(transformData3.translateY).toBe(0);
    expect(transformData3.scale).toBe(1);
    expect(transformData3.rotate2d).toBe(0);
  });

  it("should generate transform data from an array of pointers", () => {
    const transformData = TransformData.fromPointers([
      new Pointer(
        "uuid",
        { clientX: 256, clientY: 64, identifier: "mouse", scale: 1.2 },
        { device: "mouse", startTime: 0 },
      ),
    ]);

    expect(transformData.translateX).toBe(256);
    expect(transformData.translateY).toBe(64);
    expect(transformData.scale).toBe(1.2);
    expect(transformData.rotate2d).toBe(0);

    const transformData2 = TransformData.fromPointers([
      new Pointer(
        "uuid",
        { clientX: 0, clientY: 0, identifier: "t/0" },
        { device: "touch", startTime: 0 },
      ),
      new Pointer(
        "uuid",
        { clientX: 128, clientY: 128, identifier: "t/1" },
        { device: "touch", startTime: 0 },
      ),
    ]);

    expect(transformData2.translateX).toBe(64);
    expect(transformData2.translateY).toBe(64);
    expect(transformData2.scale).toBeCloseTo(90.5097);
    // TODO: Rotation
  });

  it("should create a new set of identical transform values", () => {
    const transformData = new TransformData(54, 33, 98, 0);
    const transformData2 = transformData.clone();
    expect(transformData2).not.toBe(transformData);
    expect(transformData2.translateX).toBe(transformData.translateX);
    expect(transformData2.translateY).toBe(transformData.translateY);
    expect(transformData2.scale).toBe(transformData.scale);
    expect(transformData2.rotate2d).toBe(transformData.rotate2d);
  });

  it("should invert the transform data", () => {
    const transformData = new TransformData().invert();
    expect(transformData.translateX).toBe(0);
    expect(transformData.translateY).toBe(0);
    expect(transformData.scale).toBe(1);
    expect(transformData.rotate2d).toBe(0);

    const transformData2 = new TransformData(128, 0, 2, 45).invert();
    expect(transformData2.translateX).toBe(-128);
    expect(transformData2.translateY).toBe(0);
    expect(transformData2.scale).toBe(0.5);
    expect(transformData2.rotate2d).toBe(-45);
  });

  it("should add two sets of transform data", () => {
    const transformData = new TransformData().add(
      new TransformData(128, 0, 2, 45),
    );
    expect(transformData.translateX).toBe(128);
    expect(transformData.translateY).toBe(0);
    expect(transformData.scale).toBe(2);
    expect(transformData.rotate2d).toBe(45);

    const transformData2 = transformData.add(new TransformData(0, 12));
    expect(transformData2.translateX).toBe(128);
    expect(transformData2.translateY).toBe(12);
    expect(transformData2.scale).toBe(2);
    expect(transformData2.rotate2d).toBe(45);
    expect(transformData2).toBe(transformData);
  });

  it("should subtract two sets of transform data", () => {
    const transformData = new TransformData().subtract(
      new TransformData(128, 0, 2, 45),
    );
    expect(transformData.translateX).toBe(-128);
    expect(transformData.translateY).toBe(0);
    expect(transformData.scale).toBe(0.5);
    expect(transformData.rotate2d).toBe(-45);

    const transformData2 = transformData.subtract(new TransformData(0, 12));
    expect(transformData2.translateX).toBe(-128);
    expect(transformData2.translateY).toBe(-12);
    expect(transformData2.scale).toBe(0.5);
    expect(transformData2.rotate2d).toBe(-45);
    expect(transformData2).toBe(transformData);
  });

  it("should set the transform data's values", () => {
    const transformData = new TransformData().set(
      new TransformData(128, 0, 2, 45),
    );
    expect(transformData.translateX).toBe(128);
    expect(transformData.translateY).toBe(0);
    expect(transformData.scale).toBe(2);
    expect(transformData.rotate2d).toBe(45);

    const transformData2 = transformData
      .set({ rotate2d: 90 })
      .set({ scale: 0 })
      .set({});
    expect(transformData2.translateX).toBe(128);
    expect(transformData2.translateY).toBe(0);
    expect(transformData2.scale).toBe(1);
    expect(transformData2.rotate2d).toBe(90);
    expect(transformData2).toBe(transformData);
  });
});