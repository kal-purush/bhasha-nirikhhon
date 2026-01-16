import { z } from 'zod'

export const entrySchema = z.object({
	id: z.string(),
	url: z.string(),
	name: z.string(),
	type: z.string(),
	createdAt: z.string(),
	updatedAt: z.string(),
	extra: z
		.object({
			img: z.string(),
		})
		.optional(),
})

export type Entry = z.infer<typeof entrySchema>

export const entriesSchema = z.array(entrySchema)

export type Entries = z.infer<typeof entriesSchema>