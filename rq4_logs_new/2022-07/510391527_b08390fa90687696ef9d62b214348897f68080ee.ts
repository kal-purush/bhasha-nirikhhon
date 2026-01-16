import IExtractReactionService from './IExtractReactionService';
import { WebClient } from '@slack/web-api';

export default class ExtractPraiseReactionService
    implements IExtractReactionService
{
    private readonly client: WebClient;

    constructor(client: WebClient) {
        this.client = client;
    }

    public execute(emoji_names: string[]): string[] {
        const text_emoji_names = emoji_names.filter((emoji) => emoji.includes('text'));
        return text_emoji_names.filter((emoji_name) => {
            return emoji_name.includes('erai')
            || emoji_name.includes('iikanzi')
            || emoji_name.includes('iine')
            || emoji_name.includes('iiyo')
            || emoji_name.includes('kami')
            || emoji_name.includes('mekara')
            || emoji_name.includes('mugennnokanousei')
            || emoji_name.includes('sasuga')
            || emoji_name.includes('suko')
            || emoji_name.includes('tennsaikayo')
            || emoji_name.includes('saikou')
            || emoji_name.includes('eeyann')
            || emoji_name.includes('emoi')
        });
    }
}