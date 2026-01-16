export const VIDEO_PIXELS = 224;

export interface CameraDimentions {
  [index: number]: number;
}

export class Camera {
  videoElement: HTMLVideoElement;
  snapShotCanvas: HTMLCanvasElement;
  aspectRatio: number = -1;

  constructor() {
    this.videoElement = <HTMLVideoElement>document.querySelector(".Camera");
    this.snapShotCanvas = document.createElement("canvas");
  }

  /**
   * Requests access to the camera and return a Promise with the native width
   * and height of the video element when resolved.
   *
   * @async
   * @returns {Promise<CameraDimentions>} A promise with the width and height
   * of the video element used as the camera.
   */
  async setupCamera(): Promise<CameraDimentions | null> {
    if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: false,
        video: { facingMode: "environment" }
      });
      this.videoElement.srcObject = stream;
      return new Promise(resolve => {
        this.videoElement.onloadedmetadata = () => {
          resolve([
            this.videoElement.videoWidth,
            this.videoElement.videoHeight
          ]);
        };
      });
    }
    return null;
  }

  setupVideoDimensions(width: number, height: number) {
    this.aspectRatio = width / height;
    if (width >= height) {
      this.videoElement.height = VIDEO_PIXELS;
      this.videoElement.width = this.aspectRatio * VIDEO_PIXELS;
    } else {
      this.videoElement.width = VIDEO_PIXELS;
      this.videoElement.height = VIDEO_PIXELS / this.aspectRatio;
    }
  }

  // TODO
  pauseCamera() {
    this.videoElement.pause();
  }

  // TODO
  unPauseCamera() {
    this.videoElement.play();
  }

  // TODO: hook up with the report button
  snapshot() {}
}