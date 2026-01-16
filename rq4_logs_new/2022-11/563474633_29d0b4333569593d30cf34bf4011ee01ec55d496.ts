import { renderHook } from "@testing-library/react";
import { store } from "../redux/store";
import ProviderWrapper from "../test-utils/ProviderWrapper";
import useApi from "./useApi";
import { mockOneRobot } from "../mocks/mockRobots";

const dispatchSpy = jest.spyOn(store, "dispatch");

describe("Given the useApi custom hook", () => {
  describe("When its method loadRobots is invoked", () => {
    test("Then it should invoke dispatch with loadRobots action creator and a list of 2 robots", async () => {
      const {
        result: {
          current: { loadRobotsApi },
        },
      } = renderHook(() => useApi(), {
        wrapper: ProviderWrapper,
      });

      await loadRobotsApi();

      expect(dispatchSpy).toHaveBeenCalled();
    });
  });

  describe("When its method loadRobotByIdApi is invoked", () => {
    test("Then it should invoke dispatch with loadOneRobot action creator and a list of 1 robot", async () => {
      const robotId = "2";

      const {
        result: {
          current: { loadRobotByIdApi },
        },
      } = renderHook(() => useApi(), {
        wrapper: ProviderWrapper,
      });

      await loadRobotByIdApi(robotId);

      expect(dispatchSpy).toHaveBeenCalled();
    });
  });

  describe("When its method createOneRobot is invoked", () => {
    test("Then it should invoke dispatch with createOneRobot action creator and 1 new robot data", async () => {
      const {
        result: {
          current: { createOneRobotApi },
        },
      } = renderHook(() => useApi(), {
        wrapper: ProviderWrapper,
      });

      await createOneRobotApi(mockOneRobot);

      expect(dispatchSpy).toHaveBeenCalled();
    });
  });

  describe("When its method deleteOneRobot is invoked", () => {
    test("Then it should invoke dispatch with deleteOneRobot action creator and remove the robot", async () => {
      const {
        result: {
          current: { deleteOneRobotApi },
        },
      } = renderHook(() => useApi(), {
        wrapper: ProviderWrapper,
      });

      await deleteOneRobotApi(mockOneRobot._id);

      expect(dispatchSpy).toHaveBeenCalled();
    });
  });
});