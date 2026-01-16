import { env } from "../../../../env";
import { Schema } from "../../../../types/impl/database";

export const tableName = `${env.PREFIX}_challenges`;
export const table: Schema = {
    id: {
        type: "TEXT",
        options: {
            primaryKey: true,
            default: "gen_random_uuid()",
            notNull: true,
            unique: true,
        },
    },
    guild_id: {
        type: "TEXT",
        options: {
            notNull: true,
            unique: true,
        },
    },
    message_id: {
        type: "TEXT",
        options: {
            notNull: true,
            unique: true,
        },
    },
    stage_name: {
        type: "TEXT",
        options: {
            notNull: true,
        },
    },
    stage_data: {
        type: "JSONB",
        options: {
            notNull: false,
            default: "'{}'",
        },
    },
    challenge_description: {
        type: "TEXT",
        options: {
            notNull: true,
        },
    },
    challenge_data: {
        type: "JSONB",
        options: {
            notNull: false,
            default: "'{}'",
        },
    },
    created_at: {
        type: "TIMESTAMP",
        options: {
            notNull: true,
            default: "now()",
        },
    },
};