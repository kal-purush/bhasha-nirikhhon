import {Task, TaskProvider, Worker, Action, ActionRunner, ActionDistributor, ActionDistributorResult} from './interfaces';
import {Loggable} from './util/logging';


type WrappedAction<A, R> = {a: Action<A, R>, callback: (r: R) => void};

export class Operation<A, R> extends Loggable {

  /** Changed to null when there are no more tasks */
  private taskProvider: TaskProvider<A, R> | null;

  private readonly runningTasks: Task<A, R>[] = [];
  // private readonly failedTasks: Task<A, R>[] = [];
  private readonly queuedActions: WrappedAction<A, R>[] = [];
  private readonly runningActions: WrappedAction<A, R>[] = [];
  // private readonly failedActions: Action<A, R>[] = [];

  constructor(taskProvider: TaskProvider<A, R>) {
    super('Operation');
    this.taskProvider = taskProvider;
    // Bind Methods
    this.distributeAction = this.distributeAction.bind(this);
    this.performAction = this.performAction.bind(this);
  }

  // External Methods

  public registerWorker(worker: Worker<A, R>) {
    this.log('register worker', worker.id());
    // Just start running the worker
    worker.run(this.distributeAction);
  }

  // Methods that are passed externally (and need to be bound)

  private async distributeAction(runner: ActionRunner<A, R>): Promise<ActionDistributorResult> {
    const action = await this.getNextAction();
    if (action === 'no_actions') return 'no_more_actions';
    return await runner(action.a)
    .then(result => {
      action.callback(result)
      return 'succeeded' as ActionDistributorResult;
    })
    .catch(() => 'failed' as ActionDistributorResult);
  }

  private async performAction(action: Action<A, R>): Promise<R> {
    return new Promise<R>((resolve, reject) => {
      this.queuedActions.push({
        a: action,
        callback: resolve
      })
    });
  }

  // Other Internal Methods

  private async getNextAction(): Promise<WrappedAction<A, R> | 'no_actions'> {
    while (this.queuedActions.length === 0 && this.taskProvider) {
      // No current actions, let's add a new task so that it queues actions
      // TODO: make it possible to limit the maximum number of tasks that can be active at once.
      // so that tasks that may not operate synchronously (e.g. if they require file operations)
      // don't completely fill up the queue.
      await this.startNewTask();
    }
    // TODO: NEXT: make this work when a task is "slow" requesting an action
    const action = this.queuedActions.shift();
    if (action) {
      this.runningActions.push(action);
      return action;
    }
    return 'no_actions';
  }

  private async startNewTask() {
    if (!this.taskProvider) return;
    const task = await this.taskProvider();
    if (!task) {
      // No more tasks
      this.taskProvider = null;
      return;
    }
    this.runningTasks.push(task);
    task(this.performAction)
      .then(() => {
        // TODO: Remove from running tasks
      })
      .catch(() => {
        // TODO: Remove from running tasks and move to errored tasks
      });
  }

}

export abstract class NamedWorker<A, R> extends Loggable implements Worker<A, R> {

  private readonly _id: string;

  public constructor(id: string) {
    super(id);
    this._id = id;
  }

  public id() {
    return this._id;
  }

  abstract run(distributeAction: ActionDistributor<A, R>): Promise<void>;

}

// Utility Methods

export function taskProviderFromList<A, R>(tasks: Task<A, R>[]): TaskProvider<A, R> {
  return async () => {
    const t = tasks.shift();
    if (t) return t;
  }
}
