import Adapter from 'src/common/adapter/adapter';
import { CreateCharacterDto } from './dto/create-character.dto';
import { UpdateCharacterDto } from './dto/update-character.dto';
import { Character } from './schema/character.schema';
import { Injectable } from '@nestjs/common';

@Injectable()
export default class CharacterAdapter
  implements Adapter<Character, CreateCharacterDto, UpdateCharacterDto>
{
  updateToEntity(dto: UpdateCharacterDto): Character {
    return new Character();
  }

  createToEntity(dto: CreateCharacterDto): Character {
    return {
      name: dto.name,
      race: dto.race,
      subrace: dto.subrace,
      class: dto.class,
      level: dto.level,
      ability: dto.ability,
      alignment: dto.alignment,
      skill: dto.skill,
      equipment: dto.equipment,
      feat: dto.feat,
      spell: dto.spell,
    } as Character;
  }
}