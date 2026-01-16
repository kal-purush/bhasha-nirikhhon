import MongoReader from '../db/Mongodb/MongoReader'
import { Request, Response } from 'express'
import { ReturnError, ReturnSuccess } from '../helpers/Response'
import PhotoReader from '../services/Photo/PhotoReader'

/**
 * Hndler for GET methdos
 * @param request
 * @param response
 */
export async function Fetch(
	request: Request,
	response: Response
): Promise<void> {
	const photoId = request.params.photoId ? request.params.photoId : ''
	const page = request.params.page ? Number(request.params.page) : ''
	const limit = request.params.limit ? Number(request.params.limit) : ''
	const dbToUse = new MongoReader()

	const result = await PhotoReader.Fetch(dbToUse, photoId, page, limit)

	if (result.failed) {
		ReturnError(422, response, result.error.toString())
	} else {
		ReturnSuccess(201, response, result, photoId ? false : true)
	}
}