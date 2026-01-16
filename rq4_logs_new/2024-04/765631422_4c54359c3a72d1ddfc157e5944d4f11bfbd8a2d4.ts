/*
 * This program has been developed by students from the bachelor
 * Computer Science at Utrecht University within the Software Project course.
 *
 * © Copyright Utrecht University
 * (Department of Information and Computing Sciences)
 */

import { UseIsAuthorizedState } from "../features/authentication/AuthSlice";
import { SendMessageI } from "./types";

/**
 * Abstract base class for the Broker.
 * Defines the common interface and methods for the Broker implementations.
 */
export abstract class BrokerBase {
  protected static singletonInstance: BrokerBase;

  /** Get the singleton instance of the Broker. */
  public static instance(): BrokerBase {
    if (!this.singletonInstance) {
      throw new Error("Singleton instance not initialized");
    }
    return this.singletonInstance;
  }

  public abstract setAuth(authHeader: UseIsAuthorizedState): BrokerBase;

  public abstract connect(onOpen: () => void): void;

  public abstract sendMessage(message: SendMessageI, callback?: Function): void;

  public abstract sendMessageAsync(
    message: SendMessageI
  ): Promise<Record<string, any>>;
}