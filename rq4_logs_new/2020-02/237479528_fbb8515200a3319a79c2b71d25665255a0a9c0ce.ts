import retry from 'p-retry';
import { sleep } from './sleep';

export type ProbeFn = (ports: number[]) => Promise<void>;

export interface Probe {
    // Number of seconds after the container has started before liveness or readiness probes are initiated.
    // Defaults to 0 seconds.
    // Minimum value is 0.
    initialDelaySeconds?: number;
    // How often (in seconds) to perform the probe.
    // Default to 10 seconds.
    // Minimum value is 1.
    periodSeconds?: number;
    // Number of seconds after which the probe times out.
    // Defaults to 1 second.
    // Minimum value is 1.
    timeoutSeconds?: number;
    // Minimum consecutive successes for the probe to be considered successful after having failed.
    // Defaults to 1.
    // Must be 1 for liveness.
    // Minimum value is 1
    successThreshold?: number;
    // When a Pod starts and the probe fails, it will try failureThreshold times before giving up.
    // Giving up in case of liveness probe means restarting the container.
    // In case of readiness probe the Pod will be marked Unready.
    // Defaults to 3.
    // Minimum value is 1.
    failureThreshold?: number;
    // Function that performs the probe.
    fn: ProbeFn;
}

export async function perform(p: Probe, ports: number[]): Promise<void> {
    if (p == null) {
        return Promise.reject(new Error('Missed probe configuration'));
    }

    if (typeof p.fn !== 'function') {
        return Promise.reject(new Error('Missed probe function'));
    }

    const fn = p.fn;

    if (typeof p.initialDelaySeconds === 'number') {
        await sleep(p.initialDelaySeconds * 1000);
    }

    return retry(() => fn(ports), {
        retries: p.failureThreshold || 3,
        maxRetryTime: p.timeoutSeconds || Infinity,
        maxTimeout: (p.periodSeconds || 10) * 1000,
        minTimeout: 1 * 1000,
    });
}