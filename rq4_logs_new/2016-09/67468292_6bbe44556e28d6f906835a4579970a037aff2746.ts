import { Speech } from './speech'
import { StageDirections } from './stage-directions'

export class Scene {
    speeches: Speech[];
    characters: String[];
    directions: StageDirections[];
    sceneNumber: number;

    getSpeeches(): Speech[]{
        return this.speeches;
    }

    getCharacters(): String[]{
        return this.characters;
    }

    getDirections(): StageDirections[]{
        return this.directions;
    }

    getSceneNumber(): number{
        return this.sceneNumber;
    }
}