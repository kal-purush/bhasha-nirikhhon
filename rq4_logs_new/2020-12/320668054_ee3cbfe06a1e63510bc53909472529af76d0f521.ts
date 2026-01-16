export function* init(name: string) {
	console.log(`${name}: my process has started`)

	const creep = Game.creeps[name]

	const origin = creep.memory.origin
		? creep.memory.origin
		: creep.memory.origin = creep.room.name

	while (true) {
		const creep = Game.creeps[name]

		if (!creep)
			return

		if (!creep.store.getFreeCapacity())
			yield* deposit()
		// else if (!creep.store.getUsedCapacity())
		// 	yield* collect()
		else
			yield* collect()
	}

	function* deposit() {
		while (true) {
			const creep = Game.creeps[name]

			if (!creep)
				return

			const room = Game.rooms[origin]

			let target: Structure | undefined = room.controller

			for (const spawn of room.find(FIND_MY_SPAWNS)) {
				if (spawn.store.getFreeCapacity(RESOURCE_ENERGY)) {
					target = spawn
					break
				}
			}

			if (target && creep.transfer(target, RESOURCE_ENERGY) == ERR_NOT_IN_RANGE)
				creep.moveTo(target)

			if (!creep.store.getUsedCapacity())
				return

			yield
		}
	}

	function* collect() {
		while (true) {
			const creep = Game.creeps[name]

			if (!creep)
				return

			const source = creep.pos.findClosestByPath(FIND_SOURCES)

			if (source && creep.harvest(source) == ERR_NOT_IN_RANGE)
				creep.moveTo(source)

			if (!creep.store.getFreeCapacity())
				return

			yield
		}
	}
}