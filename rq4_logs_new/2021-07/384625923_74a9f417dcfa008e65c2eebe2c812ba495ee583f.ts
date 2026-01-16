import MongoWriter from '../db/Mongodb/MongoWriter'
import MongoReader from '../db/Mongodb/MongoReader'
import { Request, Response } from 'express'
import PhotoReader from '../services/Photo/PhotoReader'
import { PhotoWriter } from '../services/Photo/PhotoWriter'
import { Mutex } from 'async-mutex'
import { ReturnError, ReturnSuccess } from '../helpers/Response'

/**
 * This file is responsible for all PATCH request handling
 */

/**
 * Handler for  creating a photo
 * @param request
 * @param response
 */
export async function Patch(
	request: Request,
	response: Response
): Promise<void> {
	const { take } = request.body
	const photoId = request.params.photoId

	const { failed, message } = await runValidation(take, photoId)

	if (failed) {
		ReturnError(422, response, message)
	} else {
		const locks = new Map()

		// Checks to see if there is a photoId on the lock
		if (!locks.has(photoId)) {
			locks.set(photoId, new Mutex())
		}

		locks
			.get(photoId)
			.acquire()
			.then(async (release: any) => {
				try {
					const dbToUse = new MongoWriter()
					const result = await PhotoWriter.Update(photoId, false, dbToUse)
					if (result) {
						ReturnSuccess(201, response, result)
					} else {
						ReturnError(422, response, 'There was a problem update')
					}
				} catch (error) {
					console.error('Lock error', error)
				} finally {
					release()
				}
			})
	}
}

/**
 * Check if it is an invalid record id
 * @param id
 * @returns
 */
async function isValidDataToFetch(id: string): Promise<any> {
	const dbToUse = new MongoReader()
	const result = await PhotoReader.Fetch(dbToUse, id)
	let message = ''
	let isValid = true
	if (result.failed === true) {
		isValid = false
		message = 'This id does not exists in our records'
	} else if (result.available === false) {
		isValid = false
		message = 'Image already taken'
	}

	return { isValid, message }
}

/**
 * Method that executes the process of validation
 * @param take
 * @param photoId
 * @returns
 */
async function runValidation(take: boolean, photoId: string): Promise<any> {
	const { isValid, message } = await isValidDataToFetch(photoId)
	let errorMessage = ''
	if (take !== true) {
		errorMessage = 'Invalid TAKE value'
	} else if (!photoId) {
		errorMessage = 'Missing record id'
	} else if (!isValid) {
		errorMessage = message
	}

	if (errorMessage != '') {
		return { failed: true, message: errorMessage }
	}

	return { failed: false }
}