
import {TigerModule, TriggerDef, WorkerDef, ServerRequestHandler} from "./types"
import { ScheduledTask } from "node-cron";


export class Trigger implements TigerModule {
  name: string;  
  moduleDef: TriggerDef;
  valid: boolean = true;
  private _handler?: ServerRequestHandler

  constructor(name: string, moduleDef: TriggerDef) {
    this.name = name;
    this.moduleDef = moduleDef;
  }

  handler(handler: ServerRequestHandler) {
    this._handler = handler;
  }

  mountOn(fn: (name: string, handler: ServerRequestHandler) => void) {
    fn(this.name, this._handler!);
  }

  destroy(): void {
    this.valid = false;
  }
}

export class Worker implements TigerModule {
  name: string;  
  moduleDef: WorkerDef;
  valid: boolean = true;

  private _task?: ScheduledTask = undefined

  constructor(name: string, moduleDef: WorkerDef) {
    this.name = name;
    this.moduleDef = moduleDef;
  }

  task(task: ScheduledTask): ScheduledTask {
    this._task = task;
    return this._task;
  }

  start(): void {
    if (this._task) {
      this._task.start();
    }
  }

  destroy(): void {
    if (this._task) {
      this._task.destroy();
    }
    this.valid = false;
  }
}