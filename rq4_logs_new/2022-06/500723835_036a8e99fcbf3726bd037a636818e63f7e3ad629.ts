import { config as ciConfig } from "./wdio.conf";
import type { Options } from '@wdio/types'

export const config: Options.Testrunner = {
    ...ciConfig,
    ... {
        services: ['docker'],
        dockerLogs: './logs',
        dockerOptions: {
            image: 'selenium/standalone-chrome',
            healthCheck: 'http://localhost:4444',
            options: {
            e: [
                'SE_NODE_OVERRIDE_MAX_SESSIONS=true',
                'SE_NODE_MAX_SESSIONS=15',
                'SCREEN_WIDTH: 1920',
                'SCREEN_HEIGHT: 1080',
            ],
            p: ['4444:4444'],
            shmSize: '2g',
            },
        },
    }
}