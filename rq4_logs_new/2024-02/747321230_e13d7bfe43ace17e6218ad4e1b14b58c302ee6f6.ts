import { Injectable } from '@nestjs/common';
import { AppConfig } from 'src/config/config.module';
import * as fs from 'fs';

@Injectable()
export class AiHistoryService {
    static inProgress: boolean = false;

    private static getPokedexData = async (): Promise<any> => {
        const pokedexData = await fs.readFileSync(AppConfig.pokedex_history_path, 'utf8');
        return JSON.parse(pokedexData);
    }


    static getPokemonData = async (name: string, questionTitle: string): Promise<string> => {
        const pokedexData = await AiHistoryService.getPokedexData();
        if (!pokedexData[name]) {
            return '';
        }
        if (!pokedexData[name][questionTitle]) {
            return '';
        }
        return pokedexData[name][questionTitle];
    }


    static addPokemonData = async (name: string, questionTitle: string, data: any): Promise<void> => {
        const pokedexData = await AiHistoryService.getPokedexData();
        if (!pokedexData[name]) {
            pokedexData[name] = {};
        }
        pokedexData[name][questionTitle] = data;
        await fs.writeFileSync(AppConfig.pokedex_history_path, JSON.stringify(pokedexData));
    }
}

/*
there will be a json file in /data/pokedex/pokedex_history.json
there will be a class which will will have 2 methods
1. read this json file and check if the needed data is there
2. open the json file and add the data there
json structure:
{
    "pokemonName": {
        "about": "the about text",
        "weaknesses": "the weaknesses text",
        "strengths": "the strengths text"
    }
    ...
}

But I do not want to have the json file always open, only when I need it to be open
*/
