import { Converter } from "../Converter"
import { Formatter } from "../Formatter"
import { State } from "../State"
import { StateEditor } from "../StateEditor"
import { Settings } from "../Settings"
import { add } from "./base"

class Handler implements Converter<string>, Formatter {
	constructor(private readonly settings: Omit<Settings, "type">) {}
	toString(data: string): string {
		return data
	}
	fromString(value: string): string | undefined {
		return value
	}
	format(unformated: StateEditor): Readonly<State> & Settings {
		return { ...unformated, type: "text", ...this.settings, autocomplete: "email" }
	}
	unformat(formated: StateEditor): Readonly<State> {
		return formated
	}
	allowed(symbol: string, state: Readonly<State>): boolean {
		return this.settings.length == undefined || this.settings.length[1] == undefined || state.value.length < this.settings.length[1]
	}
}
add("email", (settings?: any) => new Handler(settings || { }))