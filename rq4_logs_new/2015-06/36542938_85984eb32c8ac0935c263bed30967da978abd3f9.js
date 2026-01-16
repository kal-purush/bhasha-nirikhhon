// dust pile alloying
mod.addShapelessRecipe('dustBronzeSmall 4', 'dustCopperSmall', 'dustCopperSmall', 'dustCopperSmall', 'dustTinSmall');
mod.addShapelessRecipe('dustElectrumSmall 2', 'dustSilverSmall', 'dustGoldSmall');
mod.addShapelessRecipe('dustInvarSmall 3', 'dustIronSmall', 'dustIronSmall', 'dustNickelSmall');

mod.removeRecipesByResult('minecraft:compass');
mod.removeRecipesByResult('minecraft:clock');

mod.addRecipe('minecraft:compass', 3, 3, 0, 'minecraft:iron_ingot', 0, 'minecraft:iron_ingot', 'BetterOres:chunk:10', 'minecraft:iron_ingot', 0, 'minecraft:iron_ingot', 0);
mod.addRecipe('minecraft:clock', 3, 3, 0, 'minecraft:gold_ingot', 0, 'minecraft:gold_ingot', 'BetterOres:chunk:10', 'minecraft:gold_ingot', 0, 'minecraft:gold_ingot', 0);

mod.addRecipe('minecraft:torch 2', 1, 2, 'BetterOres:chunk3:2', ':stickWood');
mod.addRecipe('minecraft:torch 8', 1, 2, 'BetterOres:chunk3:3', ':stickWood');