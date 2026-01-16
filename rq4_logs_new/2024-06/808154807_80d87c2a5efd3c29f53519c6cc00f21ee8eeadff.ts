import { CreateCharacterDto } from "../dto/create-character.dto";
import { Character } from "../schema/character.schema";
import ValidateSubClasse from "./validate.subclasse";
import ValidateSubRace from "./validate.subrace";

export default class CharacterValidate {

    private readonly createCharacter: CreateCharacterDto;

    private readonly character: Character;

    constructor(createCharacter: CreateCharacterDto, character: Character) {
        this.createCharacter = createCharacter;
        this.character = character;
    }

    public async validate(): Promise<void> {

        const validate = new ValidateSubClasse(new ValidateSubRace(null));
        validate.validate(this.createCharacter, this.character);
    }
    
}