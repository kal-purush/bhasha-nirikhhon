import { SourceMapConsumer } from "source-map"
import { DynamicMap, matches } from "./lib"

export function mapError(error: Error | string): string {
	const stack = error instanceof Error
		? error.stack!
		: error

	return error.toString().split("\n")[0] + mapError.cache.get(stack)
}

export namespace mapError {
	let consumer: SourceMapConsumer | undefined

	function getConsumer() {
		return consumer = consumer || new SourceMapConsumer(require("./main.js.map"))
	}

	export const cache = new DynamicMap<string, string>(stack => {
		let o = ""

		for (const match of matches(/^\s+at\s+(.+?\s+)?\(?([0-z._\-\\\/]+):(\d+):(\d+)\)?$/gm, stack)) {
			if (match[2] === "main") {
				const pos = getConsumer().originalPositionFor({
					column: Number(match[4]),
					line: Number(match[3])
				})

				if (pos.line != null) {
					if (pos.name) {
						o += `\n    at ${pos.name} (${pos.source}:${pos.line}:${pos.column})`
					} else {
						if (match[1]) {
							// no original source file name known - use file name from given trace
							o += `\n    at ${match[1]} (${pos.source}:${pos.line}:${pos.column})`
						} else {
							// no original source file name known or in given trace - omit name
							o += `\n    at ${pos.source}:${pos.line}:${pos.column}`
						}
					}
				} else {
					// no known position
					break
				}
			} else {
				// no more parseable lines
				break
			}
		}

		return o
	})
}

export function wrap(function_: () => void) {
	return () => {
		try {
			function_()
		} catch (error) {
			if (error instanceof Error) {
				let message

				if ("sim" in Game.rooms)
					message = `Source maps don't work in the simulator - displaying original error\n${_.escape(error.stack)}`
				else
					message = _.escape(mapError(error))

				throw { message }
			} else
				throw error
		}
	}
}