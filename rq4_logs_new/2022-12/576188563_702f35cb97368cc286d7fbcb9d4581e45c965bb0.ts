import { Logger } from '@nodescript/logger';
import { dep, Mesh } from '@nodescript/mesh';
import dotenv from 'dotenv';

export abstract class BaseApp {

    @dep() logger!: Logger;

    constructor(readonly mesh: Mesh) {
        this.mesh.connect(this);
    }

    abstract start(): Promise<void>;
    abstract stop(): Promise<void>;

    async run() {
        try {
            dotenv.config();
            process.removeAllListeners();
            if (process.env.NODE_ENV === 'development') {
                dotenv.config({ path: '.env.dev' });
            }
            process.on('uncaughtException', error => {
                this.logger.error('uncaughtException', { error });
            });
            process.on('unhandledRejection', error => {
                this.logger.error('unhandledRejection', { error });
            });
            process.on('SIGTERM', async () => {
                this.logger.info('Received SIGTERM');
                await this.shutdown();
            });
            process.on('SIGINT', async () => {
                this.logger.info('Received SIGINT');
                await this.shutdown();
            });
            await this.start();
        } catch (error) {
            this.logger.error(`Failed to start ${this.constructor.name}`, { error });
            process.exit(1);
        }
    }

    async shutdown() {
        try {
            this.logger.info('Application shutting down');
            process.removeAllListeners();
            await this.stop();
            process.exit(0);
        } catch (error) {
            this.logger.error(`Failed to stop ${this.constructor.name}`, { error });
            process.exit(1);
        }
    }

}