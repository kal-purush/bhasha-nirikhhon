import { Emoji } from '../models/emoji.model'

export type EmojiDto = {
    id: number
    char: string
    code: string
    category: string
}

class EmojiService {
    private repository = Emoji

    public async findAll(category?: string): Promise<EmojiDto[]> {
        const records = await (category
            ? this.repository.findAll({
                  where: { category: category },
              })
            : this.repository.findAll())

        return records.map(r => ({
            id: r.id,
            char: r.char,
            code: r.code,
            category: r.category,
        }))
    }
}

export const emojiService = new EmojiService()