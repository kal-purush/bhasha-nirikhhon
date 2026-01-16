import PouchDB from 'pouchdb'
import pouchdbFind from 'pouchdb-find'
import { Lyric } from '../types'

PouchDB.plugin(pouchdbFind)

const db = new PouchDB('songs')

export const addDocument = async (doc: Lyric) => {
  try {
    const response = await db.put(doc)
    console.log('Document added:', response)
  } catch (error) {
    console.error('Error adding document:', error)
  }
}

export const getAllDocuments = async (): Promise<Lyric[]> => {
  try {
    const result = await db.allDocs({ include_docs: true })
    return result.rows.map((row) => {
      const doc = row.doc as unknown
      return doc as Lyric
    })
  } catch (error) {
    console.error('Error fetching documents:', error)
    return []
  }
}

export async function loadSongsIntoDb(songs: Lyric[]) {
  const batchSize = 100
  const chunks: Lyric[][] = []

  for (let i = 0; i < songs.length; i += batchSize) {
    chunks.push(songs.slice(i, i + batchSize))
  }

  try {
    for (const chunk of chunks) {
      const response = await db.bulkDocs(chunk)
      console.log(`${response.length} songs loaded successfully.`)
    }
  } catch (err) {
    console.error('Error loading songs into DB:', err)
  }
}

export async function searchSongsByTitle(title: string) {
  const regex = new RegExp(title, 'i')

  try {
    const result = await db.find({
      selector: { title: { $regex: regex } }
    })
    return result.docs
  } catch (err) {
    throw new Error('Eroare la căutarea cântărilor: ' + err)
  }
}