import { Boss as BossModel } from '../models';
import { BossInstance, BossAttribute } from '../entity/boss';
import { Boss } from '../../domain/boss';
import { Pokedex } from '../../domain/pokedex';
import * as _ from 'lodash';

export class BossRepository {
  bot: any;
  pokedex: Pokedex;

  constructor(bot, pokedex: Pokedex) {
    this.bot = bot;
    this.pokedex = pokedex;
  }

  getById(id: string): any {
    return BossModel.find({ where: { id } });
  }

  save(boss: Boss) {
    return (boss.id) ? this.update(boss) : this.create(boss);
  }

  remove(id: number) {
    return BossModel.destroy({
      where: { id }
    });
  }

  private create(boss: Boss) {
    return BossModel.create(this.getModel(boss));
  }

  private update(boss: Boss) {
    return BossModel
      .update(this.getModel(boss), {
        where: {
          id: boss.id
        },
        returning: true
      })
      .then((response: [number, BossInstance[]]) =>
        (response[0]) ? response[1].shift() : null);
  }

  getDomainObject(instance: BossInstance): Boss {
    const boss = new Boss(this.bot, this.pokedex, instance.id, instance.channel_id, instance.boss_id, instance.start, instance.location);

    boss.id = instance.id;
    boss.createdAt = instance.created_at;
    boss.updatedAt = instance.updated_at;
    boss.deletedAt = instance.deleted_at;

    if (instance.pokemon_id) {
      boss.setPokemon(_.toInteger(instance.pokemon_id));
    }

    return boss;
  }

  getModel(boss: Boss) {
    const model: BossAttribute = {
      id: boss.id,
      created_at: boss.createdAt,
      updated_at: boss.updatedAt,
      deleted_at: boss.deletedAt,
      boss_id: boss.bossId,
      channel_id: boss.channelId,
      location: boss.location,
      pokemon_id: boss.pokemonId,
      start: boss.start
    };

    return model;
  }
}