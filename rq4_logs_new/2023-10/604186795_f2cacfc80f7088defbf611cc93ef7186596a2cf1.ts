import { useEffect, RefObject } from 'react';
import Konva from 'konva';
import { CURSOR_URL } from '@static';
import { useRecoilState } from 'recoil';
import { ToolAtom } from '@recoil';

type Direction = 'up' | 'down' | 'left' | 'right';

export type useMoveParams = {
  stageRef: RefObject<Konva.Stage> | null;
  layerRef: RefObject<Konva.Layer> | null;
  time?: number;
  offset?: number;
  direction?: Direction;
  callback?: (clientX: number, clientY: number, offset: number) => void;
};

/**
 * 
 * @param params - stageRef, layerRef, time, offset, direction, callback
 * @description - 클릭시 커서 이미지에 원하는 방향으로 애니메이션을 주입합니다.
 * @example
 * const MyTool = ({ stageRef }: MyToolProps) => {
    const layerRef = useRef<Konva.Layer>(null);

    const callbackFunc = (clientX: number, clientY: number) => {
      // ...
      func1(clientX, clientY);
      func2();
    };

    useMove({ stageRef, layerRef, callback: callbackFunc });

    return (
      <Layer ref={layerRef}>
      // ...
      </Layer>
    )
  }
 */
export const useMove = (params: useMoveParams) => {
  const { stageRef, layerRef, time = 300, offset = 15, direction = 'down', callback = () => {} } = params;
  const [tool, setTool] = useRecoilState(ToolAtom);
  const toolImage = new Image();
  toolImage.src = CURSOR_URL[tool];
  useEffect(() => {
    const stage = stageRef?.current;
    const layer = layerRef?.current;
    if (!stage || !layer) return;
    const handleMouseDown = (e: Konva.KonvaEventObject<MouseEvent>) => {
      setTool('none');
      const { clientX, clientY } = e.evt;

      const movingStampImage = new Konva.Image({
        image: toolImage,
        x: clientX,
        y: clientY,
        offsetX: toolImage.width / 2,
        offsetY: toolImage.height / 2,
      });
      layer.add(movingStampImage);

      let endPosition = { x: clientX, y: clientY };

      switch (direction) {
        case 'up':
          endPosition.y = clientY - offset;
          break;
        case 'down':
          endPosition.y = clientY + offset;
          break;
        case 'left':
          endPosition.x = clientX - offset;
          break;
        case 'right':
          endPosition.x = clientX + offset;
          break;
        default:
          break;
      }

      movingStampImage.to({
        ...endPosition,
        duration: time / 2000,
        onFinish: () => {
          setTool('stamp');
          movingStampImage.remove();
        },
      });
      callback(clientX, clientY, offset);
    };

    stage.on('mousedown', handleMouseDown);

    return () => {
      stage.off('mousedown', handleMouseDown);
    };
  }, [stageRef]);
};