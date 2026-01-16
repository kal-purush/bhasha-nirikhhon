export function getTouches(e) {
  const h = e.target.clientHeight;
  const w = e.target.clientWidth;

  return [...e.touches].map((touch) => ({
    id: touch.identifier,
    h,
    w,
    x: touch.clientX,
    y: touch.clientY,
  }));
}

function getDiffs(lastTouches, touches) {
  return touches.reduce((diffs, touch) => {
    const match = lastTouches.find((last) => last.id === touch.id);
    if (match) {
      const x = touch.x - match.x;
      const y = touch.y - match.y;

      return [...diffs, { x, y }];
    }
    return diffs;
  }, []);
}

function getAverages(diffs) {
  const init = { x: 0, y: 0 };

  if (diffs.length > 0) {
    return diffs.reduce((sum, { x, y }) => ({
      x: sum.x + x,
      y: sum.y + y,
    }), init);
  }
  return init;
}

export function getMoveRatios(lastTouches, touches) {
  const w = touches[0].w;
  const h = touches[0].h;
  const diffs = getDiffs(lastTouches, touches);
  const { x, y } = getAverages(diffs);
  const xRatio = x / w;
  const yRatio = y / h;

  return {
    x: xRatio,
    y: yRatio,
  };
}