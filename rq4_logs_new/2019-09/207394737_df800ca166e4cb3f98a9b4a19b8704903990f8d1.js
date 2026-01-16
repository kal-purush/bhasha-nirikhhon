import Dexie from 'dexie'

export function initClientStorage() {
    const db = new Dexie('Vladivodico')
    db.version(1).stores({
        words: 'uuid, word'
    })

    return db
}