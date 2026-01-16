import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { PokemonAbilityController } from './pokemon-ability.controller';
import { PokemonAbilityService } from './pokemon-ability.service';
import { Test, TestingModule } from '@nestjs/testing';

import {
  OVERGROW_ABILITY_FIXTURE,
  LIST_ABILITIES_FIXTURE,
} from '@repo/mock/pokemon/fixtures/completes/abilities/index';

describe('PokemonAbilityController', () => {
  let service: PokemonAbilityService;
  let controller: PokemonAbilityController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [PokemonAbilityController],
      providers: [
        {
          provide: PokemonAbilityService,
          useValue: {
            list: jest.fn(),
            findOne: jest.fn(),
          },
        },
      ],
    }).compile();

    service = module.get<PokemonAbilityService>(PokemonAbilityService);
    controller = module.get<PokemonAbilityController>(PokemonAbilityController);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
    expect(controller).toBeDefined();
  });

  describe('findAll', () => {
    it('Should return an list of pokemon move', async () => {
      jest.spyOn(service, 'list').mockResolvedValue(LIST_ABILITIES_FIXTURE);

      expect(await controller.findAll({})).toEqual(LIST_ABILITIES_FIXTURE);
    });
  });

  describe('findOne', () => {
    it('Should return an pokemon move', async () => {
      jest.spyOn(service, 'findOne').mockResolvedValue(OVERGROW_ABILITY_FIXTURE);

      expect(await controller.findOne(OVERGROW_ABILITY_FIXTURE.name)).toEqual(
        OVERGROW_ABILITY_FIXTURE,
      );
    });
  });
});