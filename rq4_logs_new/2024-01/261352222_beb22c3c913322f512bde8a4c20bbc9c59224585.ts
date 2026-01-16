import type BS3 from "better-sqlite3";

import type { Bot } from "Bot";
import { Logger } from "Logger";

import type { CommandInteraction, Emoji, EmojiResolvable, Guild, Message, Role, Snowflake } from "discord.js";

export class StarboardConfigs {
    private readonly logger: Logger;

    private readonly db: BS3.Database;

    constructor(bot: Bot, db: BS3.Database) {
        this.logger = Logger.getLogger(this);
        this.db = db;
    }

    /**
     * @returns undefined when not found, null on db error
     */
    public getConfig(
        guildId: Snowflake,
    ): StarboardConfig | null | undefined {
        try {
            const row: StarboardConfig | undefined = this.db
                .prepare(
                    "SELECT * FROM StarboardConfigs WHERE guildId = ?;"
                )
                .get(guildId) as StarboardConfig | undefined;
            return row;
        } catch (err) {
            this.logger.error("Error getting StarboardConfigs from db", err);
            return null;
        }
    }

    public setConfig(
        guildId: Snowflake,
        channelId: Snowflake,
        emoteId: EmojiResolvable,
        numReacts: number,
        enabled: boolean,
    ): boolean {
        let rowsModified = 0;

        try {
            const info = this.db
                .prepare(
                    "INSERT INTO StarboardConfigs(guildId, enabled, channelId, emoteId, numReacts) " +
                        "VALUES(?, ?, ?, ?, ?) " +
                        "ON CONFLICT(guildId, enabled, channelId, emoteId, numReacts) " +
                        "DO UPDATE SET guildId=excluded.guildId enabled=excluded.enabled " +
                        "channelId=excluded.channelId emoteId=excluded.emoteId numReacts=excluded.numReacts;"
                )
                .run(
                    guildId,
                    enabled,
                    channelId,
                    emoteId,
                    numReacts,
                );
            rowsModified = info.changes;
        } catch (err) {
            this.logger.error("Error setting StarboardConfig", err);
        }

        return rowsModified > 0;
    }
}

export type StarboardConfig = {
    channelId: Snowflake;
    emoteId: EmojiResolvable;
    enabled: boolean;
    numReacts: number;
};
