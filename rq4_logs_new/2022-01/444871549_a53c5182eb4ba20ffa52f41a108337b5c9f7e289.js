var DZ = {};

DZ.Profile = function(name = "",speed = [0,0], range = 5, fight = 5, save = 5, armor = 0, health = 1, size = 1, base = 25,
        strikeTeamRule = null){
    this.Name = name;
    this.Speed = speed;
    this.Range = range;
    this.Fight = fight;
    this.Save = save;
    this.Armor = armor;
    this.HP = health;
    this.Size = size;
    this.Base = base;
    this.Options = [];
    this.Keywords = [];
    this.StrikeTeamRule = strikeTeamRule;
}


DZ.Weapon = function(name = "", range = 0, ap = 0){
    this.Name = name;
    this.Range = range;
    this.AP = ap;
    this.Keywords = [];
}


DZ.Option = function(name = "", type = "Troop", vp = 0, cost = 1){
    this.Name = name;
    this.Type = type;
    this.Weapons = [];
    this.VP = vp;
    this.Cost = cost;
}

//TODO: DZ.Unit - Combine the Profile + Option to create the actual model and game stats