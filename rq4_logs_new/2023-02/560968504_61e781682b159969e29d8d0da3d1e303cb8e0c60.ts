/* eslint-disable max-classes-per-file */
import { AsyncLocalStorage } from 'node:async_hooks'
import { Request } from 'express'
import { TFunction } from 'i18next'

// Helper class, for storing Request Data
export class RequestData {
	method: string
	url: string
	ip: string
	headers: any
	query: any
	body: any
	constructor(req: Request) {
		this.method = req.method
		this.url = req.originalUrl
		// NOTE: x-real-ip is from nginx reverse proxy
		this.ip = req.header('x-real-ip') || req.ip
		this.headers = req.headers
		this.query = req.query
		this.body = req.body
	}
}

// Helper interface, represent data stored in AsyncLocalStorage
export interface AsyncStorageData {
	t?: TFunction
	requestID?: string
	request?: RequestData
}

export class Flow {
	private static storage: AsyncLocalStorage<AsyncStorageData> = new AsyncLocalStorage<AsyncStorageData>()

	static get(): AsyncStorageData {
		const store = this.storage.getStore()
		if (!store) {
			throw new Error('Cannot get store, method called outside of an asynchronous context')
		}

		return store
	}
}