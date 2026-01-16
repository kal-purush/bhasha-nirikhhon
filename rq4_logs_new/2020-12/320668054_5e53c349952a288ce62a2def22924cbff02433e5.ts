import { roleBodies } from "./config"

export function buildCreep(role: string, room: Room, creeps: Creep[]) {
	const memory: CreepMemory = { origin: room.name }
	let body: BodyPartConstant[] = []

	switch (role) {
		case "harvester":
			for (var source of room.find(FIND_SOURCES)) {
				var container = source.pos.findInRange(FIND_STRUCTURES, 1, {
					filter : { structureType: STRUCTURE_CONTAINER }
				})[0];

				if (
					container &&
					creeps.every(creep => creep.memory.target != container.id)
				) {
					var costs = [ 250, 400, 500, 650, 750 ];

					body = [ WORK, MOVE ];

					for (
						var i = 0;
						i < 5 && costs[i] < room.energyAvailable;
						i++
					) {
						body.push(WORK);
						i % 2 && body.push(MOVE);
					}

					memory.target = container.id;

					break;
				}
			}

			break;
		default:
			body = [ ...roleBodies[role] ];

			var cost = body.reduce(
					(cost, part) => cost + BODYPART_COST[part],
					0
				),
				level = Math.floor((room.energyAvailable - cost) / cost);

			for (var i = 0; i < level; i++)
				body.push(...roleBodies[role]);
	}

	return [ body.sort(), `${role}_${Game.time.toString(36)}`, { memory } ];
}