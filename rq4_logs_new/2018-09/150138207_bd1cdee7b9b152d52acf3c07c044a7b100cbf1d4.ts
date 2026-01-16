export class Pokemon {
  name: String;
  hp: number;
  lvl: number;
  tipos: String[];
  sprite: String;
  constructor(
    name: string,
    hp: number,
    lvl: number,
    tipos: String[],
    sprite: String
  ) {
    this.name = name;
    this.hp = hp;
    this.lvl = lvl;
    this.tipos = tipos;
    this.sprite = sprite;
  }

  getName() {
    return this.name;
  }

  setName(name) {
    this.name = name;
  }

  getHp() {
    return this.hp;
  }

  setHp(hp) {
    this.hp = hp;
  }

  getLvl() {
    return this.lvl;
  }

  setLvl(lvl) {
    this.lvl = lvl;
  }

  getTipos() {
    return this.tipos;
  }

  setTipos(tipos) {
    this.tipos = tipos;
  }

  getSprite() {
    return this.sprite;
  }

  setSprite(sprite) {
    this.sprite = sprite;
  }

  setAllData(
    name: String,
    hp: number,
    lvl: number,
    sprite: String,
    tipos: String[]
  ) {
    this.setName(name);
    this.setHp(hp);
    this.setLvl(lvl);
    this.setSprite(sprite);
    this.setTipos(tipos);
  }
}