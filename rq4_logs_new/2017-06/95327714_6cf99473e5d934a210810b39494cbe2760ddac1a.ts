import * as Emoji from 'node-emoji';
import * as Moment from 'moment';
import * as _ from 'lodash';
import { Group } from './group';
import * as i18n from 'i18next';
import { User } from './user';
import { Pokedex } from './pokedex';
import { GroupInstance } from '../models/entity/group';

export class Boss {
  id: number;
  bossId: number;
  bot: any;
  channelId: number;
  location: string;
  name: string;
  pokemonId: number;
  pokedex: Pokedex;
  start: Date;
  groups: Group[];

  constructor(bot: any, pokedex: Pokedex, id: number, channelId: number, bossId: number, start: Date, location: string) {
    this.id = id;
    this.bot = bot;
    this.bossId = bossId;
    this.channelId = channelId;
    this.location = location;
    this.pokedex = pokedex;
    this.start = start;
    this.groups = [
      new Group(this.bot, this.id, i18n.t('team.valor'), 1),
      new Group(this.bot, this.id, i18n.t('team.mystic'), 2),
      new Group(this.bot, this.id, i18n.t('team.instinct'), 3)
    ];
  }

  setPokemon(pokemonId: number) {
    const pokemon = this.pokedex.getPokemonById(pokemonId);
    this.name = pokemon.zhHkName;
    this.pokemonId = pokemon.id;
  }

  getTeam(teamId) {
    return _.find(this.groups, group => group.seq === teamId);
  }

  addGroup(groupInstance: GroupInstance) {
    const group = _.find(this.groups, group => group.seq === groupInstance.seq);
    group.id = groupInstance.id;
    group.name = groupInstance.name;
    group.bossId = groupInstance.boss_id;

    return group;
  }

  addUserToGroup(user: User) {
    const group: Group = this.getTeam(user.teamId);
    group.addUser(user);
  }

  getFlag() {
    let flag = '';

    switch (this.pokemonId) {
      case 3: flag = Emoji.get('seedling'); break;
      case 6: flag = Emoji.get('fire'); break;
      case 9: flag = Emoji.get('droplet'); break;
      case 112: flag = Emoji.get('mountain'); break;
      case 131: flag = Emoji.get('snowflake'); break;
      case 143: flag = Emoji.get('panda_face'); break;
      case 248: flag = Emoji.get('hatched_chick'); break;
      default: flag = Emoji.get('red_circle'); break;
    }

    return flag;
  }

  removeUserInGroup(userId: number) {
    _.map(this.groups, group => {
      group.users = _.filter(group.users, user => user.id !== userId);
    });
  }

  getGroupIds() {
    return _.map(this.groups, 'id');
  }

  getEmojiName() {
    if (!this.pokemonId) return '';
    const star = (_.indexOf([131, 143, 248], this.pokemonId) > -1) ? `${Emoji.get('star')}` : '';
    return `${this.getFlag()} ${this.name} ${star}`;
  }

  toString() {
    let list = `${Moment(this.start).format('HH:mm')} ${this.location} ${this.getEmojiName()}\n\n`;

    _.map(this.groups, (group: Group) => {
      list += `${group.toString()}\n`;
    });

    return list;
  }
}