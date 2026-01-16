import signalR from "@aspnet/signalr";

/**
 * SignalR signalling server connection information.
 */
const hub = new signalR.HubConnectionBuilder()
  .configureLogging(signalR.LogLevel.Information)
  .withUrl(process.env.REACT_APP_INGEST_SIGNALR_URL as string, {
    skipNegotiation: true,
    transport: signalR.HttpTransportType.WebSockets
  })
  .build();

export default class GroupController {
  /**
   * Called when the state of record has changed (Play/Pause);
   */
  public onRecordStateChanged: (isPlaying: boolean) => void = () => {};

  /**
   * Called when the record duration has changed.
   */
  public onDurationChanged: (duration: number) => void = () => {};

  /**
   * Called when the recording sesion has changed.
   */
  public onRecordFinished: () => void = () => {};

  constructor() {
    hub.start();

    hub.on("onRecordStateChanged", this.onRecordStateChanged);
    hub.on("onDurationChanged", this.onDurationChanged);
    hub.on("onRecordFinished", this.onRecordFinished);
  }

  /**
   * Requests the current record state of the group.
   */
  public requestRecordState = () => {
    hub.invoke("onRequestRecordState");
  };

  /**
   * Sets the recording state of the group.
   */
  public setRecordState = (isRecording: boolean) => {
    hub.invoke("onSetRecordState", isRecording);
  };

  /**
   * Stops the recording session.
   */
  public stopRecording = () => {
    hub.invoke("onStopRecording");
  };

  /**
   * Starts the recording session.
   */
  public startRecording = () => {
    hub.invoke("onStartRecording");
  };
}