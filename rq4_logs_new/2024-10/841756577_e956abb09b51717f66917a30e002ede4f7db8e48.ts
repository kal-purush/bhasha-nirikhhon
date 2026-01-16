import { tableName } from "..";
import { postgres } from "../../../..";
import type { GetUserInput, User } from "../../../../../types/impl/database/impl/users";

export const get = async (input: GetUserInput): Promise<User | null> => {
    const query = `
        SELECT * FROM ${tableName}
        WHERE id = $1;
    `;

    const params = [input.id];

    try {
        const res = await postgres.query(query, params);

        if (res.rows.length > 0) {
            const user = res.rows[0] as User;
            return user;
        } else {
            return null;
        }
    } catch (e) {
        console.error(e);
        return null;
    }
};

export const getByDiscordId = async (input: GetUserInput): Promise<User | null> => {
    const query = `
        SELECT * FROM ${tableName}
        WHERE user_id = $1;
    `;

    const params = [input.user_id];

    try {
        const res = await postgres.query(query, params);

        if (res.rows.length > 0) {
            const user = res.rows[0] as User;
            return user;
        } else {
            return null;
        }
    } catch (e) {
        console.error(e);
        return null;
    }
};

export const getByGuildId = async (input: GetUserInput): Promise<User[] | null> => {
    const query = `
        SELECT * FROM ${tableName}
        WHERE guild_id = $1;
    `;

    const params = [input.guild_id];

    try {
        const res = await postgres.query(query, params);

        if (res.rows.length > 0) {
            const users = res.rows as User[];
            return users;
        } else {
            return null;
        }
    } catch (e) {
        console.error(e);
        return null;
    }
};