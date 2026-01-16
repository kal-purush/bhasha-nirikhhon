import express from 'express'

export default async function startServer(): Promise<void> {
	const app = express()

	// eslint-disable-next-line @typescript-eslint/no-var-requires
	await require('./loaders').default(app)

	app
		.listen(process.env.PORT, () => {
			// eslint-disable-next-line no-console
			console.log(`The server is running on port ${process.env.PORT}`)
		})
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		.on('error', (error: any) => {
			// eslint-disable-next-line no-console
			console.error(`Express failed: ${error}`)
		})
}

startServer()