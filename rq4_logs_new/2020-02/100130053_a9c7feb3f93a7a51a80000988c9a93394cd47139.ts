import TransformGesture from "./transformGesture";
import TransformData from "./transformData";

describe.only("TransformGesture", () => {
  it("should create a new gesture", () => {
    expect(new TransformGesture().context).toEqual({});

    expect(
      new TransformGesture(new TransformData(24, 256, 1, 0)).getTransform(),
    ).toEqual(new TransformData(0, 0, 1, 0));

    expect(
      new TransformGesture(new TransformData(), { key: "value" }).context.key,
    ).toBe("value");
  });

  it("should set compute the offset to a new target", () => {
    const gesture = new TransformGesture(new TransformData(0, 256, 1, 0));
    gesture.setTarget(new TransformData(64, 128, 4, -45));
    expect(gesture.getTransform()).toEqual(new TransformData(64, -128, 4, -45));
  });

  it.only("should rebase the gesture", () => {
    const gesture = new TransformGesture(new TransformData(128, 256, 1, 0))
      .setTarget(new TransformData(128, 128, 1, 0))
      .rebase(new TransformData(128, 256, 1, 0));

    expect(gesture.getTransform()).toEqual(new TransformData(0, -128, 1, 0));

    expect(
      gesture
        .rebase(new TransformData(64, 0, 256, 23))
        .setTarget(new TransformData(0, 0, 128, 23))
        .getTransform(),
    ).toEqual(new TransformData(-64, -128, 0.5, 0));
  });
});