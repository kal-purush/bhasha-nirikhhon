import { ICardGrabber } from './ICardGrabber'
import { ICache } from './utilities/ICache'
import { ICardInfo } from './ICardInfo'

export class CacheCardGrabber implements ICardGrabber {
    private nonCache: ICardGrabber
    private cache: ICache<string, ICardInfo>

    constructor(nonCache: ICardGrabber, cache: ICache<string, ICardInfo>) {
        this.nonCache = nonCache
        this.cache = cache
    }

    getCard = async (name: string) => {
        if (await this.cache.has(name)) {
            return await this.cache.get(name) || { name: '', img_uri: '' }
        } else {
            let info = await this.nonCache.getCard(name)
            return await this.cache.put(name, info)
        }
    }
}