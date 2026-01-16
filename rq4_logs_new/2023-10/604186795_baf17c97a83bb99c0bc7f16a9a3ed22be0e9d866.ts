import { useRecoilState } from 'recoil';
import { useAntKiller } from './useAntKiller';
import { AntAtom, AntData, Stamp, StampAtom } from '@recoil';

export const useEraser = (dx: number, dy: number) => {
  const [_, setAnts] = useRecoilState(AntAtom);
  const [__, setStamps] = useRecoilState(StampAtom);

  const eraseAnts = (x: number, y: number) => {
    const inEraserRange = (ant: AntData) => Math.abs(ant.x - x) < dx && Math.abs(ant.y - y) < dy;
    setAnts(ants => ants.filter(ant => !(inEraserRange(ant) && ant.dead)));
  };

  const eraseStamps = (x: number, y: number) => {
    const inEraserRange = (stamp: Stamp) => Math.abs(stamp.x - x) < dx && Math.abs(stamp.y - y) < dy;
    setStamps(stamps => stamps.filter(stamp => !inEraserRange(stamp)));
  };

  return { eraseAnts, eraseStamps };
};