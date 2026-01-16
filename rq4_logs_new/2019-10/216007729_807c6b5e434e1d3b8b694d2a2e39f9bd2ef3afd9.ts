import roleHarvester from './role.harvester';
import roleUpgrader from './role.upgrader';
import roleBuilder from './role.builder';
import tower from './tower';

export const loop = () => {
  console.log('tick');

  // Automatically delete memory of missing creeps
  for (const name in Memory.creeps) {
    if (!(name in Game.creeps)) {
      delete Memory.creeps[name];
      console.log('Clearing non-existing creep memory:', name);
    }
  }

  tower();

  // Handle spawned creeps
  const harvesters = Object.keys(Game.creeps).filter(creep => Game.creeps[creep].memory.role === 'harvester');
  const upgraders = Object.keys(Game.creeps).filter(creep => Game.creeps[creep].memory.role === 'upgrader');
  const builders = Object.keys(Game.creeps).filter(creep => Game.creeps[creep].memory.role === 'builder');

  Game.spawns['Spawn1'].room.visual.text(
    harvesters.length + ' harvesters, ' + upgraders.length + ' upgraders, ' + builders.length + ' builders.',
    Game.spawns['Spawn1'].pos.x,
    Game.spawns['Spawn1'].pos.y + 5,
    { align: 'left', opacity: 0.8 },
  );

  if (harvesters.length < 10) {
    const newName = Game.spawns['Spawn1'].createCreep([WORK, WORK, CARRY, CARRY, MOVE, MOVE, MOVE, MOVE], undefined, { role: 'harvester' });
    if (typeof newName === 'string') {
      console.log('Spawning new harvester: ' + newName);
    }
  } else if (upgraders.length < 5) {
    const newName = Game.spawns['Spawn1'].createCreep([WORK, WORK, CARRY, CARRY, MOVE, MOVE, MOVE, MOVE], undefined, { role: 'upgrader' });
    if (typeof newName === 'string') {
      console.log('Spawning new upgrader: ' + newName);
    }
  } else if (builders.length < 3) {
    const newName = Game.spawns['Spawn1'].createCreep([WORK, WORK, CARRY, CARRY, MOVE, MOVE, MOVE, MOVE], undefined, { role: 'builder' });
    if (typeof newName === 'string') {
      console.log('Spawning new builder: ' + newName);
    }
  }

  const spawning = Game.spawns['Spawn1'].spawning;
  if (spawning) {
    const spawningCreep = Game.creeps[spawning.name];
    Game.spawns['Spawn1'].room.visual.text(
      '🛠️' + spawningCreep.memory.role,
      Game.spawns['Spawn1'].pos.x + 1,
      Game.spawns['Spawn1'].pos.y,
      { align: 'left', opacity: 0.8 },
    );
  }

  for (const name in Game.creeps) {
    const creep = Game.creeps[name];
    if (creep.memory.role === 'harvester') {
      roleHarvester.run(creep);
    }
    if (creep.memory.role === 'upgrader') {
      roleUpgrader.run(creep);
    }
    if (creep.memory.role === 'builder') {
      roleBuilder.run(creep);
    }
  }
};