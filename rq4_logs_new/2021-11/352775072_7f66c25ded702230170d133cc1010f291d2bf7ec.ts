import SQLite from '../sqlite'

import { SearchCache } from './cache'

export async function list_words() {
	const db = await get_db()
	const rows = await db.query<{ id: string }>(`SELECT id FROM history ORDER BY time DESC`)
	await db.close()
	return rows.map((x) => x.id)
}

export async function add_word(id: string) {
	const db = await get_db()
	const time = Date.now()
	await db.query(`INSERT OR REPLACE INTO history (id, time) VALUES (?, ?)`, [id, time])
	await db.close()
	SearchCache.update((x) => {
		if (x.id == id) {
			x.saved = true
		}
	})
	SearchCache.remove_with_history_flag()
}

export async function remove_word(id: string) {
	const db = await get_db()
	await db.query(`DELETE FROM history WHERE id = ?`, [id])
	await db.close()
	SearchCache.update((x) => {
		if (x.id == id) {
			x.saved = false
		}
	})
	SearchCache.remove_with_history_flag()
}

async function get_db() {
	const db = await SQLite.open('word_history.db')
	await db.exec(`
		CREATE TABLE IF NOT EXISTS history (
			id   TEXT PRIMARY KEY,
			time INT
		);
	`)
	return db
}