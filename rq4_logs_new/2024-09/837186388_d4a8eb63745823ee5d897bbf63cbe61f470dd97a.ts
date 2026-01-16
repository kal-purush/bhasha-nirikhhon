import * as THREE from "three";
import { DataToPosConverter } from "../../components/features/AR/Converter";
import { FC, memo } from "react";
import { useSocketRefStore } from "../../store/useSocketRefStore";

type Position = "front" | "left" | "right" | "back";
type AllObject = {
  BlockSet: THREE.Mesh[];

  stage: THREE.Mesh;
};
type Props = {
  position: Position;
  allBlockSet: React.MutableRefObject<AllObject | null>;
};
export const ObjectSetting: FC<Props> = memo(({ position, allBlockSet }) => {
  const objects = useSocketRefStore((state) => state.eventState.objetcts);

  DataToPosConverter(position, objects, allBlockSet);
  return null;
});