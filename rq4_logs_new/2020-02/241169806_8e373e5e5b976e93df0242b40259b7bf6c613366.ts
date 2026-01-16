import { Formatter } from "./Formatter"
import { Settings } from "./Settings"
import { State } from "./State"
import { StateEditor } from "./StateEditor"

export interface Action {
	key: string
	repeat?: boolean
	ctrlKey?: boolean
	shiftKey?: boolean
	altKey?: boolean
	metaKey?: boolean
}
export namespace Action {
	export function apply(formatter: Formatter, state: Readonly<State>, action?: Action): Readonly<State> & Settings {
		const result = State.copy(formatter.unformat(StateEditor.copy(state)))
		if (action) {
			if (action.key == "ArrowLeft") {
				result.selection.start -= result.selection.start > 0 ? 1 : 0
				if (!action.shiftKey) // no create or extend selection
					result.selection.end = result.selection.start
			} else if (action.key == "ArrowRight") {
				result.selection.end += result.selection.end < result.value.length ? 1 : 0
				if (!action.shiftKey) // no create or extend selection
					result.selection.start = result.selection.end
			} else if (action.key == "Home") {
				result.selection.start = 0
				if (!action.shiftKey) // no create or extend selection
					result.selection.end = result.selection.start
			} else if (action.key == "End") {
				result.selection.end = result.value.length
				if (!action.shiftKey) // no create or extend selection
					result.selection.start = result.selection.end
			} else if (action.ctrlKey) {
				switch (action.key) {
					case "a":
						result.selection.start = 0
						result.selection.end = result.value.length
				}
			} else {
				if (result.selection.start != result.selection.end) { // selection exists
					switch (action.key) {
						case "Delete":
						case "Backspace":
							action = undefined
							break
						default:
							break
					}
					result.value = result.value.substring(0, result.selection.start) + result.value.substring(result.selection.end)
					result.selection.end = result.selection.start
				}
				if (action)
					switch (action.key) {
						case "Unidentified": break
						case "Backspace":
							if (result.selection.start > 0) {
								result.value = result.value.substring(0, result.selection.start - 1) + result.value.substring(result.selection.start)
								result.selection.start = --result.selection.end
							}
							break
						case "Delete":
							if (result.selection.start < result.value.length)
								result.value = result.value.substring(0, result.selection.start) + result.value.substring(result.selection.start + 1)
							break
						default:
							if (formatter.allowed(action.key, result)) {
								result.value = result.value.substring(0, result.selection.start) + action.key + result.value.substring(result.selection.start)
								result.selection.start = result.selection.end += action.key.length
							}
					}
			}
		}
		return formatter.format(StateEditor.copy(result))
	}

}