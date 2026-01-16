import * as Dnd from '@dnd-kit/core';
import type { HierarchyData } from '~web/services';

export function useDndContextProps(): Dnd.DndContextProps {
  const sensors = Dnd.useSensors(
    Dnd.useSensor(Dnd.MouseSensor, {
      activationConstraint: {
        distance: 10,
      },
    }),
    Dnd.useSensor(Dnd.TouchSensor, {
      activationConstraint: {
        delay: 250,
        tolerance: 5,
      },
    })
  );

  return {
    sensors,
    onDragEnd: ({ active, over }) => console.log('onDragEnd', active, over),
  };
}

export function useDroppable(data: HierarchyData<string>, disabled = false) {
  const { _id: id, type } = data;
  const drop = Dnd.useDroppable({ id, disabled: disabled || type !== 'group' });

  return [drop.setNodeRef, drop.isOver] as const;
}

export function useDraggable(data: HierarchyData<string>, disabled = false) {
  const { _id: id } = data;
  const { active } = Dnd.useDndContext();
  const drag = Dnd.useDraggable({ id, disabled });

  return [
    drag.setNodeRef,
    drag.isDragging,
    {
      style: {
        opacity: !active || active.id === id ? 1 : 0.6,
        transform: !drag.transform
          ? undefined
          : `translate3d(${drag.transform.x}px, ${drag.transform.y}px, 0) scale(0.9)`,
      },
      ...(!disabled && {
        ...drag.attributes,
        ...drag.listeners,
      }),
    },
  ] as const;
}