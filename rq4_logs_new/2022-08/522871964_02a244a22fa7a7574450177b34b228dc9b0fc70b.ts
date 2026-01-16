import "reflect-metadata";
import {Character} from "../typeDefs/character";
import {Arg, Mutation, Query, Resolver} from "type-graphql";
import prisma from "../../prisma/client";
import {Race} from '@prisma/client';

@Resolver(Character)
export class CharacterResolver {

    @Query(() => [Character])
    async getAllCharacters() {
        return prisma.character.findMany()
    }

    @Query(() => [Character], {nullable: true})
    async getUserCharacters(userId: number) {
        return prisma.character.findMany({
            where: {
                userId: userId
            }
        })
    }

    @Mutation((returns) => Character)
    async createCharacter(
        @Arg("email")userId: number,
        @Arg("firstName") firstName: string,
        @Arg("lastName") lastName: string,
        @Arg("age") age: number,
        @Arg("race") race: Race,
        @Arg("bio") bio: string
    ) {
        return prisma.character.create({
            data: {
                firstName,
                lastName,
                age,
                race,
                bio,
                userId
            }
        })
    }
}