import * as AsyncHooks from '@creditkarma/async-hooks'
import {
    IAsyncMap,
    IAsyncNode,
    IAsyncOptions,
    IAsyncScope,
} from './types'
import * as Utils from './utils'

import {
    NODE_EXPIRATION,
    PURGE_INTERVAL,
} from './constants'

export class AsyncScope implements IAsyncScope {
    public static debug(msg: string, ...args: Array<any>): void {
        AsyncHooks.debug(msg, ...args)
    }

    private asyncMap: IAsyncMap
    private nodeExpiration: number
    private purgeInterval: number
    private lastPurge: number

    constructor({
        nodeExpiration = NODE_EXPIRATION,
        purgeInterval = PURGE_INTERVAL,
    }: IAsyncOptions = {}) {
        const self = this
        this.asyncMap = {}
        this.nodeExpiration = nodeExpiration
        this.purgeInterval = purgeInterval
        this.lastPurge = Date.now()

        AsyncHooks.createHook({
            init(asyncId: number, type: string, triggerAsyncId: number, resource: object) {
                // AsyncScope.debug(`asyncId[${asyncId}], parentId[${triggerAsyncId}]`)
                if (self.asyncMap[triggerAsyncId] === undefined) {
                    self.addNode(triggerAsyncId, null)
                }

                const parentNode: IAsyncNode | undefined = self.asyncMap[triggerAsyncId]

                if (parentNode !== undefined) {
                    parentNode.children.push(asyncId)
                    parentNode.timestamp = Date.now()
                    self.addNode(asyncId, triggerAsyncId)
                }

                self.purge()
            },
            before(asyncId: number) {
                // Nothing to see here
                // AsyncScope.debug(`before[${asyncId}]`)
            },
            after(asyncId: number) {
                // Nothing to see here
                // AsyncScope.debug(`after[${asyncId}]`)
            },
            promiseResolve(asyncId: number) {
                // Nothing to see here
                // AsyncScope.debug(`promiseResolve[${asyncId}]`)
            },
            destroy(asyncId: number) {
                Utils.destroyNode(asyncId, self.asyncMap)

                self.purge()
            },
        }).enable()
    }

    public get<T>(key: string): T | null {
        const activeId: number = AsyncHooks.executionAsyncId()
        this.addNode(activeId, null)
        return Utils.recursiveGet<T>(key, activeId, this.asyncMap)
    }

    public set<T>(key: string, value: T): void {
        const activeId: number = AsyncHooks.executionAsyncId()
        this.addNode(activeId, null)
        const activeNode: IAsyncNode | undefined = this.asyncMap[activeId]
        if (activeNode !== undefined) {
            activeNode.data[key] = value
        }
    }

    public delete(key: string): void {
        const activeId: number = AsyncHooks.executionAsyncId()
        Utils.recursiveDelete(key, activeId, this.asyncMap)
    }

    /**
     * A method for debugging, returns the lineage (parent scope ids) of the current scope
     */
    public lineage(): Array<number> {
        const activeId: number = AsyncHooks.executionAsyncId()
        return Utils.lineageFor(activeId, this.asyncMap)
    }

    private addNode(asyncId: number, parentId: number | null): void {
        if (this.asyncMap[asyncId] === undefined) {
            this.asyncMap[asyncId] = {
                id: asyncId,
                timestamp: Date.now(),
                parentId,
                exited: false,
                data: {},
                children: [],
            }
        }
    }

    private purge(): void {
        const currentTime: number = Date.now()
        if ((currentTime - this.lastPurge) > this.purgeInterval) {
            this.lastPurge = currentTime
            Utils.runPurge(this.asyncMap, currentTime, this.nodeExpiration)
        }
    }
}