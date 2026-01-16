import type { DeleteGuildInput } from "../../../../../types/impl/database/impl/guilds";
import { tableName } from "..";
import { postgres } from "../../../..";
import emitter, { Events } from "../../../../../events";
import colors from "colors";

/**
 * Deletes a guild entry from the database by ID.
 * Emits an event upon successful deletion.
 */
export const deleteItem = async (data: DeleteGuildInput) => {
    const { id } = data;

    try {
        // Begin a transaction
        await postgres.query("BEGIN");

        // Delete the guild by its ID
        const result = await postgres.query(`DELETE FROM ${tableName} WHERE id = $1 RETURNING *`, [id]);

        // If no rows were affected, the ID was not found
        if (result.rowCount === 0) {
            throw new Error(`Guild with ID ${id} not found`);
        }

        // Commit the transaction
        await postgres.query("COMMIT");

        // Emit a deletion event with the deleted data
        emitter.emit(Events.DATABASE_GUILDS_DELETE, result.rows[0]);
        return result.rows[0]; // Optionally return the deleted guild
    } catch (error) {
        // Rollback the transaction on error
        await postgres.query("ROLLBACK");
        console.error(colors.red(`Failed to delete guild with ID ${id}:`), error);
        throw error;
    }
};