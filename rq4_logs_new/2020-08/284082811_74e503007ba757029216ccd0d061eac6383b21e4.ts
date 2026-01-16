import {
  Observable,
  Scene,
  Texture
} from 'babylonjs';

/**
 * Improves Babylon's `Texture` class with consistent loading, loaded, and error observables
 * when updating the image data.
 */
export default class UpdatableTexture extends Texture {
  /**
   * Observable triggered when there is an error loading the texture.
   */
  public readonly onErrorObservable: Observable<Error> = new Observable();
  /**
   * Observable triggered when beginning to load a new texture.
   */
  public readonly onLoadingObservable: Observable<string> = new Observable();
  private _isLoading: boolean = false;
  private _updateCounter: number = 0;

  constructor(
    url: string,
    scene: Scene,
    noMipmap?: boolean,
    invertY?: boolean,
    samplingMode?: number
  ) {
    super(url, scene, noMipmap, invertY, samplingMode);
  }

  /**
   * Returns true if the texture is currently loading.
   */
  public get isLoading(): boolean {
    return this._isLoading;
  }

  /**
   * Updates the texture with the contents of a new URL in place. If called while loading a texture,
   * the most recent URL will be loaded.
   * @param url - the new URL
   */
  public updateURL(url: string): void {
    if (url === this.url) {
      return;
    }

    this._isLoading = true;
    this.onLoadingObservable.notifyObservers(url);

    const currentUpdate: number = ++this._updateCounter;
    const img: HTMLImageElement = document.createElement('img');
    img.src = url;
    img.onload = () => {
      if (currentUpdate === this._updateCounter) {
        this._isLoading = false;
        super.updateURL(url, img);
        this.onLoadObservable.notifyObservers(this);
      }
      img.remove();
    };
    img.onerror = () => {
      if (currentUpdate === this._updateCounter) {
        this._isLoading = false;
        this.onErrorObservable.notifyObservers(new Error(`Error loading image ${url}`));
      }
      img.remove();
    }
  }
}