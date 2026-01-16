import { chromium } from 'playwright';

import { DatabaseUtility } from '../db/db'

export class Scraper {
  private db: DatabaseUtility;

  constructor(db: DatabaseUtility) {
    this.db = db;
  }

  async ensureTableExists() {
    const createWeeklyRacesTableSql = `
      CREATE TABLE IF NOT EXISTS weekly_races (
        id varchar(45) NOT NULL,
        race_name varchar(255) NOT NULL,
        race_place varchar(255) DEFAULT NULL,
        number_of_entries int DEFAULT NULL,
        race_state varchar(255) DEFAULT NULL,
        date varchar(255) DEFAULT NULL,
        PRIMARY KEY (id),
        INDEX id_index (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    `;
    const createRaceEntriesTableSql = `
      CREATE TABLE IF NOT EXISTS race_entries (
        horse_id varchar(45) NOT NULL,
        race_id varchar(45) NOT NULL,
        box varchar(45) DEFAULT NULL,
        horse_order varchar(45) DEFAULT NULL,
        horse_name varchar(45) DEFAULT NULL,
        sex_and_age varchar(45) DEFAULT NULL,
        burden_weight varchar(45) DEFAULT NULL,
        jockey varchar(45) DEFAULT NULL,
        horse_weight varchar(45) DEFAULT NULL,
        horse_trainer varchar(45) DEFAULT NULL,
        horse_owner varchar(90) DEFAULT NULL,
        PRIMARY KEY (horse_id),
        KEY FK_race_entries_race_id (race_id),
        CONSTRAINT FK_race_entries_race_id FOREIGN KEY (race_id) REFERENCES weekly_races (id)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    `;
    try {
      await this.db.query(createWeeklyRacesTableSql);
      console.log('weekly_races table is ready.');
    } catch (error) {
      console.error('Error weekly_races table:', error);
    }
    try {
      await this.db.query(createRaceEntriesTableSql);
      console.log('race_entries table is ready.');
    } catch (error) {
      console.error('Error race_entries table:', error);
    }
  }

  async scrapeRaces(date: string) {
    const browser = await chromium.launch({ headless: false });
    const page = await browser.newPage();
    await page.goto(`https://race.netkeiba.com/top/?kaisai_date=${date}`);

    const races = await page.locator(`//span[@class="RaceList_ItemLong Turf"]/parent::div/parent::div/parent::a`).all();
    for (const race of races) {
      await race.click();
      await this.processRace(page);
      await page.goBack();
    }

    await browser.close();
  }

  private async processRace(page: any) {
    const id = await this.extractId(page);
    const raceName = await this.extractTextContent(page, `//div[@class="RaceName"]`);
    const racePlace = await this.extractTextContent(page, `//ul[contains(@class, "Col")]/li[@class="Active"]`);
    const numberOfEntries = (await page.locator(`//tbody/tr[@class="HorseList"]`).all()).length;
    const raceState = await this.extractTextContent(page, `//div[@class="RaceData01"]`);
    const date = await this.extractTextContent(page, `//dd[@class="Active"]`);

    const weeklyRace = { id, raceName, racePlace, numberOfEntries, raceState, date };
    await this.insertRace(weeklyRace);
    await this.scrapeEntries(page, id)
  }

  private async insertRace(race: any) {
    const sql = "INSERT INTO weekly_races (id, race_name, race_place, number_of_entries, race_state, date) VALUES (:id, :raceName, :racePlace, :numberOfEntries, :raceState, :date)";
    try {
      await this.db.query(sql, race);
      await this.db.query("select * from weekly_races")
      console.log('Race inserted:', race);
    } catch (error) {
      console.error('Error inserting race:', error);
    }
  }

  private async scrapeEntries(page: any, id:string) {
    const HORSE_LIST_SELECTOR = '//tr[@class="HorseList"]'
    const horses = await page.locator(HORSE_LIST_SELECTOR).all();

    for (const [index, horse] of Object.entries(horses)) {
      const horseId = id + index.padStart(2, '0');
      const horseOrder = await this.extractTextContent(horse, `//td[@class="Umaban Txt_C"]`);
      const box = await this.extractTextContent(horse, `//td[@class="Waku Txt_C"]`);
      const horseName = await this.extractTextContent(horse, `//span[@class="HorseName"]`);
      const sexAndAge = await this.extractTextContent(horse, `//td[@class="Barei Txt_C"]`);
      const burdenWeight = await this.extractTextContent(horse, `//td[@class="Txt_C"]`);
      const jockey = await this.extractTextContent(horse, `//td[@class="Jockey"]`);
      const horseWeight = await this.extractTextContent(horse, `//td[@class="Weight"]`);
      const horseTrainer = "";
      const horseOwner = "";

      const entryHorse = { 
        horseId, id, box, horseOrder, horseName, sexAndAge, burdenWeight, 
        jockey, horseWeight, horseTrainer, horseOwner
      };
      await this.insertEntrie(entryHorse);
    }
  }
  private async insertEntrie(horse: any) {
    const sql = "INSERT INTO race_entries (horse_id, race_id, box, horse_order, horse_name, sex_and_age, burden_weight, jockey, horse_weight, horse_trainer, horse_owner) VALUES (:horseId, :id, :box, :horseOrder, :horseName, :sexAndAge, :burdenWeight, :jockey, :horseWeight, :horseTrainer, :horseOwner)";
    try {
      await this.db.query(sql, horse);
      await this.db.query("select * from race_entries")
      console.log('Horse inserted:', horse);
    } catch (error) {
      console.error('Error inserting horse:', error);
    }
  }

  private async extractId(page: any): Promise<string> {
    let id = await page.locator(`//link[@rel="canonical"]`).getAttribute('href');
    return id ? id.slice(-12) : '';
  }

  private async extractTextContent(page: any, selector: string): Promise<string> {
    let content = await page.locator(selector).textContent();
    return content ? content.replace(/\n/g, '') : '';
  }
}