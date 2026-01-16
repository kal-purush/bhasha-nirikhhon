import { tableName } from "..";
import { postgres } from "../../../..";
import type { GetGuildInput, Guild } from "../../../../../types/impl/database/impl/guilds";

export const get = async (input: GetGuildInput): Promise<Guild | null> => {
    const query = `
        SELECT * FROM ${tableName}
        WHERE id = $1;
    `;

    const params = [input.id];

    try {
        const res = await postgres.query(query, params);

        if (res.rows.length > 0) {
            const guild = res.rows[0] as Guild;
            return guild;
        } else {
            return null;
        }
    } catch (e) {
        console.error(e);
        return null;
    }
};