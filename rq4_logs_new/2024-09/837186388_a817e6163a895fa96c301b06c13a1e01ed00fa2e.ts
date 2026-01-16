import type { HandLandmarkerResult } from "@mediapipe/tasks-vision";
import type { HandInfo } from "../Converter";
import type { AllObject } from "../Converter";

export const handBlockCatch = (
  status: "front" | "left" | "right" | "back",
  handResultRef: React.RefObject<HandLandmarkerResult>,
  allBlockSet: React.RefObject<AllObject>,
  worldHandInfo: HandInfo
) => {
  const catchDist = 1;
  if (worldHandInfo.handStatus) {
    if (
      handResultRef.current &&
      handResultRef.current.landmarks.length > 0 &&
      allBlockSet.current
    ) {
      const handPos = {
        x: (handResultRef.current.landmarks[0][0].x - 0.5) * 7,
        y: (handResultRef.current.landmarks[0][0].z + 0.4) * 10 - 3,
        z: (handResultRef.current.landmarks[0][0].y - 0.5) * 10 - 2,
      };
      //statusによって距離測定するブロックを変更
      //frontだったら0~4、leftだったら5~9、rightだったら10~14、backだったら15~19
      let distance = catchDist;
      let blockIndex = null;
      switch (status) {
        case "front":
          for (let i = 0; i < 5; i++) {
            const block = allBlockSet.current?.frontBlockSet[i];
            const blockPos = block.position;
            const blockDistance = Math.sqrt(
              (handPos.x - blockPos.x) ** 2 +
                (handPos.y - blockPos.y) ** 2 +
                (handPos.z - blockPos.z) ** 2
            );
            console.log(`${blockDistance}です`);
            if (blockDistance <= distance) {
              distance = blockDistance;
              blockIndex = i;
            }
          }
          break;
        case "left":
          for (let i = 0; i < 5; i++) {
            const block = allBlockSet.current?.leftBlockSet[i];
            const blockPos = block.position;
            const blockDistance = Math.sqrt(
              (handPos.x - blockPos.x) ** 2 +
                (handPos.y - blockPos.y) ** 2 +
                (handPos.z - blockPos.z) ** 2
            );
            if (blockDistance <= distance) {
              distance = blockDistance;
              blockIndex = i;
            }
          }
          break;
        case "right":
          for (let i = 0; i < 5; i++) {
            const block = allBlockSet.current?.rightBlockSet[i];
            const blockPos = block.position;
            const blockDistance = Math.sqrt(
              (handPos.x - blockPos.x) ** 2 +
                (handPos.y - blockPos.y) ** 2 +
                (handPos.z - blockPos.z) ** 2
            );
            if (blockDistance <= distance) {
              distance = blockDistance;
              blockIndex = i;
            }
          }
          break;
        case "back":
          for (let i = 0; i < 5; i++) {
            const block = allBlockSet.current?.backBlockSet[i];
            const blockPos = block.position;
            const blockDistance = Math.sqrt(
              (handPos.x - blockPos.x) ** 2 +
                (handPos.y - blockPos.y) ** 2 +
                (handPos.z - blockPos.z) ** 2
            );
            if (blockDistance <= distance) {
              distance = blockDistance;
              blockIndex = i;
            }
          }
          break;
      }
      console.log(distance, blockIndex);
      if (blockIndex !== null) {
        console.log("catch", status, blockIndex);
        switch (status) {
          case "front":
            allBlockSet.current.frontBlockSet[blockIndex].position.set(
              handPos.x,
              handPos.y,
              handPos.z
            );
            break;
          case "left":
            allBlockSet.current.leftBlockSet[blockIndex].position.set(
              handPos.x,
              handPos.y,
              handPos.z
            );
            break;
          case "right":
            allBlockSet.current.rightBlockSet[blockIndex].position.set(
              handPos.x,
              handPos.y,
              handPos.z
            );
            break;
          case "back":
            allBlockSet.current.backBlockSet[blockIndex].position.set(
              handPos.x,
              handPos.y,
              handPos.z
            );
            break;
        }
      }
    }
  }
};