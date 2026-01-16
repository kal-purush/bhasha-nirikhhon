import Vector from './vector';
import { ICoords } from './interfaces/i-coords';

export function netForce(forces: Vector[]): Vector {
  // creates a new zeroed vector
  let _f = new Vector();
  // every force is additive to _f
  forces.forEach((force) => {
    _f.add(force.val);
  });
  // returns forces sum vector
  return _f;
  // this.applyForce(_f);
}

// a force if a vector that causes an object with mass to accelerate
// Net Force = Mass * Acceleration\
export function applyForce(mass: Vector, acc: Vector, force: Vector): Vector {
  // zero out acceleration between frames
  acc.multi(0);
  // acceleration = net force / mass
  force.divide(mass.val);
  acc.add(force.val);

  return acc;
}

export function applyKinetics(forces: Vector[], velocity: Vector, acc: Vector, maxVelocity?: ICoords | number): Vector {
  netForce(forces);

  // TL;DR Newtons three laws of Motion
  // 1. an object at rest stays at rest and an object in motion stays in motion
  velocity.add(acc.val);

  if (maxVelocity) {
    velocity.limit(maxVelocity);
  }

  return velocity;
}