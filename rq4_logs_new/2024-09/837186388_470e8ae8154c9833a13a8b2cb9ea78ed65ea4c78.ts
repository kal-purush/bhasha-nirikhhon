import type { AllObject } from "../Converter";
import type { Tumiki } from "../../../../pages/AR";
import type * as THREE from "three";

export const tumikiSystem = (
  status: "front" | "left" | "right" | "back",
  tumikiRef: React.RefObject<Tumiki>,
  allBlockSet: React.RefObject<AllObject>
) => {
  //配列の最後の要素を取得
  if (tumikiRef.current) {
    const lastBlockIndex =
      tumikiRef.current.overlapedBlockIndex[
        tumikiRef.current.overlapedBlockIndex.length - 1
      ];
    if (allBlockSet.current) {
      let lastBrock: THREE.Mesh;
      let lastBlockPos: THREE.Vector3;
      switch (status) {
        case "front": {
          lastBrock = allBlockSet.current.frontBlockSet[lastBlockIndex];
          lastBlockPos = lastBrock.position;
          const lastBlockTop = {
            x: lastBlockPos.x,
            y: lastBlockPos.y,
            z: lastBlockPos.z - 0.3,
          };
          for (let i = 0; i < 5; i++) {
            if (tumikiRef.current.isOverlap[i]) {
              continue;
            }
            const block = allBlockSet.current.frontBlockSet[i];
            const blockPos = block.position;
            const blockBottom = {
              x: blockPos.x,
              y: blockPos.y,
              z: blockPos.z + 0.3,
            };
            const blockDistance = Math.sqrt(
              (lastBlockTop.x - blockBottom.x) ** 2 +
                (lastBlockTop.y - blockBottom.y) ** 2 +
                (lastBlockTop.z - blockBottom.z) ** 2
            );
            if (blockDistance <= 0.3) {
              tumikiRef.current.isOverlap[i] = true;
              allBlockSet.current.frontBlockSet[i].position.set(
                lastBlockTop.x,
                lastBlockTop.y,
                lastBlockTop.z - 0.3
              );
              tumikiRef.current.overlapedBlockIndex.push(i);
            }
          }
          break;
        }
        case "left": {
          lastBrock = allBlockSet.current.leftBlockSet[lastBlockIndex];
          lastBlockPos = lastBrock.position;
          const lastBlockTop = {
            x: lastBlockPos.x,
            y: lastBlockPos.y,
            z: lastBlockPos.z - 0.3,
          };
          for (let i = 0; i < 5; i++) {
            if (tumikiRef.current.isOverlap[i + 5]) {
              continue;
            }
            const block = allBlockSet.current.leftBlockSet[i];
            const blockPos = block.position;
            const blockBottom = {
              x: blockPos.x,
              y: blockPos.y,
              z: blockPos.z + 0.3,
            };
            const blockDistance = Math.sqrt(
              (lastBlockTop.x - blockBottom.x) ** 2 +
                (lastBlockTop.y - blockBottom.y) ** 2 +
                (lastBlockTop.z - blockBottom.z) ** 2
            );
            if (blockDistance <= 0.3) {
              tumikiRef.current.isOverlap[i] = true;
              allBlockSet.current.leftBlockSet[i].position.set(
                lastBlockTop.x,
                lastBlockTop.y,
                lastBlockTop.z - 0.3
              );
              tumikiRef.current.overlapedBlockIndex.push(i);
            }
          }
          break;
        }
        case "right": {
          lastBrock = allBlockSet.current.rightBlockSet[lastBlockIndex];
          lastBlockPos = lastBrock.position;
          const lastBlockTop = {
            x: lastBlockPos.x,
            y: lastBlockPos.y,
            z: lastBlockPos.z - 0.3,
          };
          for (let i = 0; i < 5; i++) {
            if (tumikiRef.current.isOverlap[i + 10]) {
              continue;
            }
            const block = allBlockSet.current.rightBlockSet[i];
            const blockPos = block.position;
            const blockBottom = {
              x: blockPos.x,
              y: blockPos.y,
              z: blockPos.z + 0.3,
            };
            const blockDistance = Math.sqrt(
              (lastBlockTop.x - blockBottom.x) ** 2 +
                (lastBlockTop.y - blockBottom.y) ** 2 +
                (lastBlockTop.z - blockBottom.z) ** 2
            );
            if (blockDistance <= 0.3) {
              tumikiRef.current.isOverlap[i] = true;
              allBlockSet.current.rightBlockSet[i].position.set(
                lastBlockTop.x,
                lastBlockTop.y,
                lastBlockTop.z - 0.3
              );
              tumikiRef.current.overlapedBlockIndex.push(i);
            }
          }
          break;
        }
        case "back": {
          lastBrock = allBlockSet.current.backBlockSet[lastBlockIndex];
          lastBlockPos = lastBrock.position;
          const lastBlockTop = {
            x: lastBlockPos.x,
            y: lastBlockPos.y,
            z: lastBlockPos.z - 0.3,
          };
          for (let i = 0; i < 5; i++) {
            if (tumikiRef.current.isOverlap[i + 15]) {
              continue;
            }
            const block = allBlockSet.current.backBlockSet[i];
            const blockPos = block.position;
            const blockBottom = {
              x: blockPos.x,
              y: blockPos.y,
              z: blockPos.z + 0.3,
            };
            const blockDistance = Math.sqrt(
              (lastBlockTop.x - blockBottom.x) ** 2 +
                (lastBlockTop.y - blockBottom.y) ** 2 +
                (lastBlockTop.z - blockBottom.z) ** 2
            );
            if (blockDistance <= 0.3) {
              tumikiRef.current.isOverlap[i] = true;
              allBlockSet.current.backBlockSet[i].position.set(
                lastBlockTop.x,
                lastBlockTop.y,
                lastBlockTop.z - 0.3
              );
              tumikiRef.current.overlapedBlockIndex.push(i);
            }
          }
          break;
        }
      }
    }
  }
};