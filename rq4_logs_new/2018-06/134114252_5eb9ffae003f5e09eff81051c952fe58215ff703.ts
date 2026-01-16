import { IGeneticsItem, IGenetics } from './interfaces/i-genetics';
import {
  ICoords
} from './interfaces/i-coords';
import {
  getDistanceBetweenR2Vectors,
  randomIntBetween
} from './utils';

export default class Genetics implements IGenetics {
  public curGeneration: [IGeneticsItem];
  public preGeneration: [IGeneticsItem];
  public count: number = 0;

  constructor(curGeneration: [IGeneticsItem]) {
    this.cycleGeneration(curGeneration);
  }

  // bumps the current generation to previous
  // and adds the new generation to current
  public cycleGeneration(generation: [IGeneticsItem]) {
    this.preGeneration = this.curGeneration;
    this.curGeneration = generation;
  }

  // TODO: confirm this math checks out
  public getScore(vector: ICoords, targetVector: ICoords, originVector: ICoords): number {
    let _originFromTarget = getDistanceBetweenR2Vectors(originVector, targetVector);
    let _unitFromTarget = getDistanceBetweenR2Vectors(vector, targetVector);

    return (1 - (_unitFromTarget / _originFromTarget)) * (100);
  }

  // to be run every time a new population is generated
  // the goal being the highest probablity will go to
  // parents with the highest score all while still
  // allowing generations from lower scoring parents
  public getProbability(a: [number]): [number] {
    let sum: number = a.reduce((n1, n2) => n1 + n2);
    let prob: any = [];

    a.forEach((score) => {
      prob.push(score / sum * 100);
    });

    return prob;
  }
}