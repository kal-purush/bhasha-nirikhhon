import { Test, TestingModule } from '@nestjs/testing';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { Repository } from 'typeorm';
import { getRepositoryToken } from '@nestjs/typeorm';

import { BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE } from '@repo/mock/pokemon/fixtures/completes/index';

import { Ability } from '../entities/ability.entity';

import { AbilityService } from './ability.service';

describe('TypeService', () => {
    let service: AbilityService;
    let repository: Repository<Ability>;

    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            providers: [
                AbilityService,
                { provide: getRepositoryToken(Ability), useClass: Repository },
            ],
        }).compile();

        service = module.get<AbilityService>(AbilityService);
        repository = module.get<Repository<Ability>>(getRepositoryToken(Ability));
    });

    it('should be defined', () => {
        expect(service).toBeDefined();
        expect(repository).toBeDefined();
    });

    describe('findList(entityType)', () => {
        it('should return a list of Pokémon types from the database', async () => {

            BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE.abilities.forEach((ability) => {
                jest.spyOn(repository, 'createQueryBuilder').mockReturnValueOnce({
                    andWhere: jest.fn(),
                    getOne: jest.fn().mockReturnValueOnce(ability),
                } as any);
            })

            expect(await service.findList(BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE.abilities)).toEqual(
                BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE.abilities,
            );
        });

        it('must save a list of pokemon types in the database when none exist', async () => {

            BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE.abilities.forEach(() => {
                jest.spyOn(repository, 'createQueryBuilder').mockReturnValueOnce({
                    andWhere: jest.fn(),
                    getOne: jest.fn().mockReturnValueOnce(null),
                } as any);
            })

            BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE.abilities.forEach((ability) => {
                jest
                    .spyOn(repository, 'save')
                    .mockResolvedValueOnce(ability);
            })

            BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE.abilities.forEach((ability) => {
                jest.spyOn(repository, 'createQueryBuilder').mockReturnValueOnce({
                    andWhere: jest.fn(),
                    getOne: jest.fn().mockReturnValueOnce(ability),
                } as any);
            })

            expect(await service.findList(BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE.abilities)).toEqual(
                BULBASAUR_ENTITY_COMPLETE_POKEMON_FIXTURE.abilities,
            );
        });
    });
});