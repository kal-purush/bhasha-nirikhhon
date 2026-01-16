interface angle {
  x: number;
  y: number;
  z: number;
}

interface scale {
  x: number;
  y: number;
  z: number;
}

interface position {
  x: number;
  y: number;
  z: number;
}

interface objectInfo {
  position: position;
  angle: angle;
  scale: scale;
}

function worldToFront(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const frontX = x;
  const frontY = y - 0.8;
  const frontZ = -z + 0.8;
  return {
    x: frontX,
    y: frontY,
    z: frontZ,
  };
}

function frontToWorld(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const worldX = x;
  const worldY = y + 0.8;
  const worldZ = -z - 0.8;
  return {
    x: worldX,
    y: worldY,
    z: worldZ,
  };
}

function worldToRight(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const rightX = y;
  const rightY = -x - 0.8;
  const rightZ = -z + 0.8;
  return {
    x: rightX,
    y: rightY,
    z: rightZ,
  };
}

function rightToWorld(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const worldX = -y + 0.8;
  const worldY = x;
  const worldZ = -z - 0.8;
  return {
    x: worldX,
    y: worldY,
    z: worldZ,
  };
}

function worldToLeft(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const leftX = -y;
  const leftY = x - 0.8;
  const leftZ = -z + 0.8;
  return {
    x: leftX,
    y: leftY,
    z: leftZ,
  };
}

function leftToWorld(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const worldX = y + 0.8;
  const worldY = -x;
  const worldZ = -z - 0.8;
  return {
    x: worldX,
    y: worldY,
    z: worldZ,
  };
}

function worldToBack(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const backX = -x;
  const backY = -y - 0.8;
  const backZ = z + 0.8;
  return {
    x: backX,
    y: backY,
    z: backZ,
  };
}

function backToWorld(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const worldX = -x;
  const worldY = -y + 0.8;
  const worldZ = z - 0.8;
  return {
    x: worldX,
    y: worldY,
    z: worldZ,
  };
}

function worldToFrontAngle(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const worldX = x;
  const worldY = y;
  const worldZ = -z;
  return {
    x: worldX,
    y: worldY,
    z: worldZ,
  };
}

function frontToWorldAngle(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const frontX = x;
  const frontY = y;
  const frontZ = -z;
  return {
    x: frontX,
    y: frontY,
    z: frontZ,
  };
}

function worldToRightAngle(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const rightX = y;
  const rightY = -x;
  const rightZ = -z;
  return {
    x: rightX,
    y: rightY,
    z: rightZ,
  };
}

function rightToWorldAngle(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const worldX = -y;
  const worldY = x;
  const worldZ = -z;
  return {
    x: worldX,
    y: worldY,
    z: worldZ,
  };
}

function worldToLeftAngle(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const leftX = -y;
  const leftY = x;
  const leftZ = -z;
  return {
    x: leftX,
    y: leftY,
    z: leftZ,
  };
}

function leftToWorldAngle(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const worldX = y;
  const worldY = -x;
  const worldZ = -z;
  return {
    x: worldX,
    y: worldY,
    z: worldZ,
  };
}

function worldToBackAngle(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const backX = -x;
  const backY = -y;
  const backZ = -z;
  return {
    x: backX,
    y: backY,
    z: backZ,
  };
}

function backToWorldAngle(
  x: number,
  y: number,
  z: number
): { x: number; y: number; z: number } {
  const worldX = -x;
  const worldY = -y;
  const worldZ = -z;
  return {
    x: worldX,
    y: worldY,
    z: worldZ,
  };
}

function worldObjectToFront(object: objectInfo): { object: objectInfo } {
  const frontPos = worldToFront(
    object.position.x,
    object.position.y,
    object.position.z
  );
  const frontAngle = worldToFrontAngle(
    object.angle.x,
    object.angle.y,
    object.angle.z
  );
  return {
    object: {
      position: frontPos,
      angle: frontAngle,
      scale: object.scale,
    },
  };
}

function worldObjectToLeft(object: objectInfo): { object: objectInfo } {
  const leftPos = worldToLeft(
    object.position.x,
    object.position.y,
    object.position.z
  );
  const leftAngle = worldToLeftAngle(
    object.angle.x,
    object.angle.y,
    object.angle.z
  );
  return {
    object: {
      position: leftPos,
      angle: leftAngle,
      scale: object.scale,
    },
  };
}

function worldObjectToRight(object: objectInfo): { object: objectInfo } {
  const rightPos = worldToRight(
    object.position.x,
    object.position.y,
    object.position.z
  );
  const rightAngle = worldToRightAngle(
    object.angle.x,
    object.angle.y,
    object.angle.z
  );
  return {
    object: {
      position: rightPos,
      angle: rightAngle,
      scale: object.scale,
    },
  };
}

function worldObjectToBack(object: objectInfo): { object: objectInfo } {
  const backPos = worldToBack(
    object.position.x,
    object.position.y,
    object.position.z
  );
  const backAngle = worldToBackAngle(
    object.angle.x,
    object.angle.y,
    object.angle.z
  );
  return {
    object: {
      position: backPos,
      angle: backAngle,
      scale: object.scale,
    },
  };
}

function frontObjectToWorld(object: objectInfo): { object: objectInfo } {
  const worldPos = frontToWorld(
    object.position.x,
    object.position.y,
    object.position.z
  );
  const worldAngle = frontToWorldAngle(
    object.angle.x,
    object.angle.y,
    object.angle.z
  );
  return {
    object: {
      position: worldPos,
      angle: worldAngle,
      scale: object.scale,
    },
  };
}

function leftObjectToWorld(object: objectInfo): { object: objectInfo } {
  const worldPos = leftToWorld(
    object.position.x,
    object.position.y,
    object.position.z
  );
  const worldAngle = leftToWorldAngle(
    object.angle.x,
    object.angle.y,
    object.angle.z
  );
  return {
    object: {
      position: worldPos,
      angle: worldAngle,
      scale: object.scale,
    },
  };
}

function rightObjectToWorld(object: objectInfo): { object: objectInfo } {
  const worldPos = rightToWorld(
    object.position.x,
    object.position.y,
    object.position.z
  );
  const worldAngle = rightToWorldAngle(
    object.angle.x,
    object.angle.y,
    object.angle.z
  );
  return {
    object: {
      position: worldPos,
      angle: worldAngle,
      scale: object.scale,
    },
  };
}

function backObjectToWorld(object: objectInfo): { object: objectInfo } {
  const worldPos = backToWorld(
    object.position.x,
    object.position.y,
    object.position.z
  );
  const worldAngle = backToWorldAngle(
    object.angle.x,
    object.angle.y,
    object.angle.z
  );
  return {
    object: {
      position: worldPos,
      angle: worldAngle,
      scale: object.scale,
    },
  };
}

export {
  worldObjectToFront,
  worldObjectToLeft,
  worldObjectToRight,
  worldObjectToBack,
  worldToFrontAngle,
  worldToLeftAngle,
  worldToRightAngle,
  worldToBackAngle,
  frontObjectToWorld,
  leftObjectToWorld,
  rightObjectToWorld,
  backObjectToWorld,
  frontToWorldAngle,
  leftToWorldAngle,
  rightToWorldAngle,
  backToWorldAngle,
  type angle,
  type scale,
  type position,
  type objectInfo,
};