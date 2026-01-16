import { IJellyfishState } from './interfaces';

import Jellyfish from '../Jellyfish';

export default class JellyfishState implements IJellyfishState {
  private _name : string;
  private _jellyfish: Jellyfish;

  protected constructor(stateName: string, jellyfish: Jellyfish) {
    this._jellyfish = jellyfish;

    this._name = stateName;
  }

  public get name(): string {
    return this._name;
  }

  protected get jellyfish(): Jellyfish {
    return this._jellyfish;
  }

  protected set jellyfish(jellyfish: Jellyfish) {
    this._jellyfish = jellyfish;
  }

  public planula(): void {
    this._blockedStateWarning('planula');
  }

  public polyp(): void {
    this._blockedStateWarning('polyp');
  }

  public polypColony(): void {
    this._blockedStateWarning('polyp colony');
  }

  public strobila(): void {
    this._blockedStateWarning('strobila');
  }

  public ephyra(): void {
    this._blockedStateWarning('ephyra');
  }

  public adultJellyfish(): void {
    this._blockedStateWarning('adult jellyfish');
  }

  private _blockedStateWarning(stateName: string): void {
    console.warn(`! ${this._name} cannot be transformed to ${stateName}`);
  }

}