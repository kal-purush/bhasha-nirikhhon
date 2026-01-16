export enum Direction {
  LEFT,
  RIGHT,
  UP,
  DOWN,
}

const SHOWN_ELEM_PORTION = 0.5;

export function findElemBeyondVisibility(direction: Direction, cnt: HTMLElement, nameDataAttr: string): HTMLElement | null {
  const children = cnt.querySelectorAll<HTMLElement>(`[${nameDataAttr}]`);
  const rect = cnt.getBoundingClientRect();

  switch (direction) {
    case Direction.LEFT:
      return findLeft(rect.left, children);
    case Direction.RIGHT:
      return findRight(rect.right, children);
    case Direction.UP:
      return null;
    case Direction.DOWN:
      return null;

    default:
      return null;
  }
}

function findLeft(left: number, children: NodeListOf<HTMLElement>): HTMLElement | null {
  let elem: HTMLElement | null = null;
  let max: number = 100000;

  children.forEach(it => {
    const rect = it.getBoundingClientRect();
    const dist = left - rect.left - rect.width * SHOWN_ELEM_PORTION;

    if (dist >= 0 && dist < max) {
      elem = it;
      max = dist;
    }
  });

  if (!elem && children[0]) {
    return children[0];
  }

  return elem;
}

function findRight(right: number, children: NodeListOf<HTMLElement>): HTMLElement | null {
  let elem: HTMLElement | null = null;
  let max: number = 100000;

  children.forEach(it => {
    const rect = it.getBoundingClientRect();
    const dist = rect.right - rect.width * SHOWN_ELEM_PORTION - right;

    if (dist >= 0 && dist < max) {
      elem = it;
      max = dist;
    }
  });

  if (!elem && children[children.length - 1]) {
    return children[children.length - 1];
  }

  return elem;
}