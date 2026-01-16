import fs from 'fs';
import readline from 'readline';

export default class ReadConfig {
    public static async execute(file_name: string): Promise<string[]> {
        const stream = fs.createReadStream(
            `${__dirname}/../config/${file_name}.txt`
        );
        const reader = readline.createInterface({ input: stream });

        const target_emoji_names: string[] = [];
        for await (const line of reader) {
            target_emoji_names.push(line);
        }
        return target_emoji_names;
    }
}