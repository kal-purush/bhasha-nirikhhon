import fs from "node:fs/promises"
import axios from "axios"
import * as cheerio from "cheerio"
import theMonsterList from "./data/sunbreak.ts"

import {
	ElementalWeakness,
	MonsterMaterialDropChance,
	MonsterMaterialDrop,
	Monster,
} from "./utils/types.ts"

const BASE_URL = "https://monsterhunterrise.wiki.fextralife.com"

function cleanText(str: string): string {
	return str
		.trim()
		.replaceAll("&amp;", "&")
		.replaceAll("&nbsp;", " ")
		.replace(/\s+/g, " ")
}

function getCampaign(monsterName: string): string {
	const monster = theMonsterList.find((m) => m.name === monsterName)

	return monster ? monster.campaign : "rise"
}

function parseMaterialDropText(str: string): MonsterMaterialDropChance[] {
	const components = str.split(/,\s+/g)
	const result = []

	for (let c of components) {
		if (c === "--") {
			continue
		}

		const chanceMatch = c.match(/(\d+)%/)

		if (!chanceMatch) {
			continue
		}

		const ch = new MonsterMaterialDropChance()

		ch.chance = parseInt(chanceMatch[1])

		const parenthesesMatch = c.match(/\(([^)]+)\)/)
		const bracketsMatch = c.match(/\[([^)]+)\]/)

		for (let m of [parenthesesMatch, bracketsMatch]) {
			if (m) {
				const str = m[1]
				if (/^x/.test(str)) {
					ch.count = parseInt(str.slice(1))
				} else {
					ch.part = str
				}
			}
		}

		result.push(ch)
	}

	return result
}

async function scrapeMonsterDetails(monsterName: string, wikiUrl: string) {
	// get HTML from wiki
	const html = await getHTML("https:" + wikiUrl, monsterName)
	if (!html) {
		throw new Error(`Failed to fetch HTML for ${monsterName}`)
	}
	const $ = cheerio.load(html)
	// find H3.bonfire with text "<Monster name> weaknesses"
	const headings = $("h3")
	let $weaknessesHeading = null

	const monster = new Monster(monsterName)
	monster.campaign = getCampaign(monsterName)

	headings.each(function () {
		if ($(this).text().includes("Weaknesses")) {
			$weaknessesHeading = $(this)
		}
	})

	if (!$weaknessesHeading) {
		throw `Invalid HTML for monster ${monsterName}`
	}

	const $subMain = $weaknessesHeading.next().next()
	const $weaknessesTable = $subMain.find("table").eq(1)
	const $weaknesssesRows = $weaknessesTable.find("tr").slice(1)

	const weaknessParts = []
	const totalWeakness = new ElementalWeakness()
	totalWeakness.name = "Total"

	for (let row of $weaknesssesRows) {
		const part = new ElementalWeakness()
		part.name = $(row).find("th").eq(0).text().trim()
		part.fire = parseInt($(row).find("td").eq(0).text().trim())
		part.water = parseInt($(row).find("td").eq(1).text().trim())
		part.thunder = parseInt($(row).find("td").eq(2).text().trim())
		part.ice = parseInt($(row).find("td").eq(3).text().trim())
		part.dragon = parseInt($(row).find("td").eq(4).text().trim())

		totalWeakness.fire += part.fire
		totalWeakness.water += part.water
		totalWeakness.thunder += part.thunder
		totalWeakness.ice += part.ice
		totalWeakness.dragon += part.dragon

		weaknessParts.push(part)
	}

	monster.elementalWeaknesses.total = totalWeakness
	monster.elementalWeaknesses.parts = weaknessParts

	const ranks = ["Low", "High", "Master"]

	for (let rank of ranks) {
		let $rankHeading = null
		const search = `${rank} Rank`.toLowerCase()

		headings.each(function () {
			// some of the titles contain &nbsp; entities
			const clearText = cleanText($(this).html()).toLowerCase()

			if (clearText.includes(search)) {
				$rankHeading = $(this)
			}
		})

		if (!$rankHeading) {
			console.warn(
				`Heading for ${monsterName} ${rank} Rank materials not found`,
			)
			continue
		}

		const $table = $rankHeading.next().next().find("table")
		const $rows = $table.find("tr").slice(1)

		for (let row of $rows) {
			const $row = $(row)
			const $cells = $row.find("td")

			const name = cleanText($cells.eq(0).text())
			const material = new MonsterMaterialDrop(name, rank)

			material.questReward = parseMaterialDropText(
				cleanText($cells.eq(1).text()),
			)
			material.captureReward = parseMaterialDropText(
				cleanText($cells.eq(2).text()),
			)
			material.partBreakReward = parseMaterialDropText(
				cleanText($cells.eq(3).text()),
			)
			material.carve = parseMaterialDropText(
				cleanText($cells.eq(4).text()),
			)
			material.drop = parseMaterialDropText(
				cleanText($cells.eq(5).text()),
			)

			monster.materials.push(material)
		}
	}

	return monster
}

async function getHTML(url: string, cacheKey: string = null) {
	if (cacheKey) {
		try {
			const cacheFile = `./cache/${cacheKey}.cache`
			const stat = await fs.stat(cacheFile)
			console.log(`[HIT] ${cacheKey}`)
			return await fs.readFile(cacheFile, "utf-8")
		} catch (err) {}
	}

	console.log(`[MISS] ${cacheKey}`)
	console.log(`>> GET >> ${url}`)

	try {
		const response = await axios.get(url)

		if (cacheKey) {
			const cacheFile = `./cache/${cacheKey}.cache`
			await fs.writeFile(cacheFile, response.data)
		}

		return response.data
	} catch (err) {
		console.error(`Error fetching ${url}`)
		return null
	}
}

const mainPageHtml = await getHTML(
	BASE_URL + "/Large+Monsters",
	"LargeMonsters",
)

const $ = cheerio.load(mainPageHtml)

const $monsterLinks = $("#tagged-pages-container a")
const c = $monsterLinks.length
const monsters = []

for (let index = 0; index < c; index++) {
	const el = $($monsterLinks.eq(index))
	const url = el.attr("href")
	const name = el.text().trim().replace(/\s+/g, " ")

	// if (name.includes("Aknosom")) {
	const monsterDetails = await scrapeMonsterDetails(name, url)
	monsters.push(monsterDetails)
	// }
}

await fs.writeFile(
	"data/monster-data.json",
	JSON.stringify(monsters, null, 2),
	"utf-8",
)