import { Jellyfish, IJellyfishState } from '../Jellyfish';

export default class AdultJellyfishState implements IJellyfishState {
  public jellyfish: Jellyfish;

  public planula(): void {
    throw new Error('Not implemented');
  }

  public polyp(): void {
    throw new Error('Not implemented');
  }

  public polypColony(): void {
    throw new Error('Not implemented');
  }

  public strobila(): void {
    throw new Error('Not implemented');
  }

  public ephyra(): void {
    throw new Error('Not implemented');
  }

  public adultJellyfish(): void {
    throw new Error('Not implemented');
  }
}