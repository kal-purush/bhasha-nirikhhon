import { info } from "../utils"
import { Creep, memoryCleaner, pixelGenerator, Room, Spawn } from "./programs"

export const processes = new Set<Generator>()
export const processQueue: Generator[] = []

export namespace Programs {
}

for (const name in Game.rooms)
	addProcess(Room(name))

for (const name in Game.creeps)
	addProcess(Creep(name))

for (const name in Game.spawns)
	addProcess(Spawn(name))

addProcess(memoryCleaner())
addProcess(pixelGenerator())
// addProcess(programs.processCount())

info(`started ${processes.size} processes`)

export function runProcess(process: Generator) {
	processes.add(process)

	if (!processQueue.includes(process))
		processQueue.push(process)
}

export function addProcess(process: Generator) {
	processes.add(process)
}