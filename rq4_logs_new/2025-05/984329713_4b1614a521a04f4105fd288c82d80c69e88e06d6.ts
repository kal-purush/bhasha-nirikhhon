import { DataTypes, Model } from "@sequelize/core";
import { Attribute, Table } from "@sequelize/core/decorators-legacy";
import { PrimaryKeyGuid } from "@/db/decorators";

@Table({
	tableName: "audits",
	createdAt: "created_at",
	updatedAt: false,
})
export class AuditModel extends Model {
	@PrimaryKeyGuid()
	declare id: string;

	@Attribute(DataTypes.STRING)
	declare user_id: string;

	@Attribute(DataTypes.STRING)
	declare table_name: string;

	@Attribute(DataTypes.ENUM(["insert", "update", "delete"]))
	declare action: "insert" | "update" | "delete";

	@Attribute(DataTypes.STRING)
	declare payload: string;

	@Attribute(DataTypes.DATE)
	declare created_at: Date;
}