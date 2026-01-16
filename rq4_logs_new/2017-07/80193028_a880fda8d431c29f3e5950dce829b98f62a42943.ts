/**
 * Provides symbol for current progress step
 */
export class ProgressStep {
    private parts: string[] = process.platform === 'win32' ?
        ['-', '\\', '|', '/'] :
        ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
    private index: number = 0;

    /**
     * Get next symbol
     */
    next(): string {
        return this.parts[this.index = ++this.index % this.parts.length];
    }
}

const defaultProgress: ProgressStep = new ProgressStep();
/**
 * Get next progress state
 */
export function progress(): string {
    return defaultProgress.next();
}

/**
 * Describes Progress object
 */
export class Progress {
    private id: NodeJS.Timer;
    private interval: number;
    private progressStep: ProgressStep;
    private callback: (step: string) => void;
    private inProgress: boolean = false;
    constructor(callback: (step: string) => void, progressStep: ProgressStep = defaultProgress, interval: number = 80) {
        this.callback = callback;
        this.progressStep = progressStep;
        this.interval = interval;
    }
    /**
     * Start timer 
     */
    start(): void {
        if (this.inProgress) {
            // There is no reason to create another timer
            return;
        }

        this.id = setInterval(function () {
            this.callback(this.progressStep.next());
        }.bind(this), this.interval);
        this.inProgress = true;
    }
    /**
     * Disable timer
     */
    stop(): void {
        if (this.inProgress) {
            clearInterval(this.id);
            this.inProgress = false;
            this.callback('⠀');
        }
    }
}

/**
 * Default progress id
 */
export const systemProgressId: string = '_system';

/**
 * Describes and provides methods for working with queue of Progress objects
 */
export class ProgressQueue {
    private updateVisibility: (visible: boolean) => void;
    private ids: Array<string> = [];
    constructor(updateVisibility: (visible: boolean) => void) {
        this.updateVisibility = updateVisibility;
    }
    /**
     * Add id to queue
     * @param id progress id
     */
    enqueue(id: string): void {
        this.ids.push(id);
        this.updateVisibility(true);
    }
    /**
     * Remove id from queue
     * @param id progress id
     */
    dequeue(id: string): void {
        let index = this.ids.indexOf(id);
        if (index>= 0) {
            this.ids.splice(index, 1);
        }
        this.update(id);
    }

    /**
     * Update progress in queue
     * @param id progress id
     */
    update(id: string): boolean {
        let index = this.ids.indexOf(id);
        let systemIndex = this.ids.indexOf(systemProgressId);
        let condition = index >= 0 || systemIndex >= 0;
        this.updateVisibility(condition);
        return condition;
    }
}

// TODO: Simplify logic.
/**
 * Primary class, manager of progress
 */
export class ProgressManager {
    /**
     * Current progress
     */
    progress: Progress;

    /**
     * Progress queue
     */
    queue: ProgressQueue;

    /**
     * Method for updating visibility of progress
     */
    visibility: (visible: boolean) => void;
    
    constructor(visibility: (visible: boolean) => void, text: (step: string) => void) {
        this.queue = new ProgressQueue(visibility);
        this.progress = new Progress(text);
    }
    /**
     * Update progress state
     * @param id progress id
     * @param inProgress Active or not
     */
    update(id: string, inProgress?: boolean): void {
        // Progress is unknown
        if (inProgress === null) {
            if (this.queue.update(id)) {
                // Display progress
                this.progress.start();
                this.visibility(true);
            } else {
                // Hide progress
                this.progress.stop();
                this.visibility(false);
            }
            return;
        }

        // Progress is known
        if (inProgress) {
            // Start progress
            this.progress.start();
            this.queue.enqueue(id);
        } else {
            // Stop progress
            this.progress.stop();
            this.queue.dequeue(id);
        }
    }
}