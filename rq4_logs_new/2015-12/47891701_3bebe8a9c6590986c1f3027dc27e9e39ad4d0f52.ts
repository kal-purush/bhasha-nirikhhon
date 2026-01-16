import {IStorageService} from './interface/storageService.ts'
import {Item} from "../model/item";

export class PouchDBService implements IStorageService<Item> {
    findAll():Item[] {
        return [
            {
                id: 1,
                name: 'http://www.fullstackradio.com/31',
                tags: ['architecture', 'estimate', 'design'],
            },
            {
                id: 2,
                name: 'http://www.fullstackradio.com/31',
                tags: ['architecture', 'estimate', 'design'],
            }
        ]
    }

    findById(id:String):Item {
        return {
            id: 1,
            name: 'http://www.fullstackradio.com/31',
            tags: ['architecture', 'estimate', 'design'],
        }
    }
}