export class AudioRecorder {
  private micStream: MediaStream | null = null;
  private recorder: MediaRecorder | null = null;

  public isRecording: boolean = false;
  public isPaused: boolean = false;

  /**
   * Pauses the recording session.
   */
  public pause = () => {
    if (this.recorder && this.recorder.state != "inactive")
      this.recorder.pause();
    this.isPaused = true;
  };

  /**
   * Resumes the recording session.
   */
  public resume = () => {
    if (this.recorder && this.recorder.state != "inactive")
      this.recorder.resume();
    this.isPaused = false;
  };

  /**
   * Starts recording the mic audio.
   */
  public start = () => {
    this.getRecorder().then(recorder => {
      this.recorder = recorder;
      recorder.start(1000);
    });
    this.isRecording = true;
  };

  /**
   * Stops recording the incoming mic audio.
   */
  public stop = () => {
    if (this.recorder) this.recorder.stop();
    if (this.micStream) this.micStream.getTracks().forEach(t => t.stop());
    this.isRecording = false;
    this.isPaused = false;
  };

  /**
   * Creates a media recorder from the provided source.
   */
  private getRecorder(): Promise<MediaRecorder> {
    return this.getMedia().then(m => {
      this.micStream = m;
      var recorder = new MediaRecorder(m);
      recorder.ondataavailable = e => this.onRecievedAudioData(e);
      return recorder;
    });
  }

  /**
   * Prompts the user for local device access.
   */
  private getMedia = (): Promise<MediaStream> => {
    return navigator.mediaDevices.getUserMedia({ audio: true, video: false });
  };

  /**
   * Called when new microphone data is available.
   */
  private onRecievedAudioData = (event: BlobEvent) => {
    console.log(event.data);
  };
}