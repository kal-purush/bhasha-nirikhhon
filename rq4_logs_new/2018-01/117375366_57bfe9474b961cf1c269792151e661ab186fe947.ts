/**
 * Definition of error and informational messages for the LocalStorageService.
 */
export class LocalStorageMessage {

  /**
   * Message to return when local Window DOM object is unavailable.
   * @type {string}
   */
  public static readonly WINDOW_UNAVAILABLE = 'Window does not exist in this environment.';

  /**
   * Message to return when Window LocalStorage is unavailable.
   * @type {string}
   */
  public static readonly LOCAL_STORAGE_UNAVAILABLE = 'Window.LocalStorage does not exist in this environment.';

  /**
   * Message to return when Window SessionStorage is unavailable.
   * @type {string}
   */
  public static readonly SESSION_STORAGE_UNAVAILABLE = 'Window.SessionStorage does not exist in this environment.';

}