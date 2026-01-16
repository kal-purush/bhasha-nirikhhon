import {
	AllowNull,
	AutoIncrement,
	Column,
	DataType,
	Length,
	Model,
	PrimaryKey,
	Table,
	TableOptions
} from 'sequelize-typescript';

@Table({
	timestamps: true,
	createdAt: 'created_at',
	updatedAt: false,
	paranoid: false,
	tableName: 'user_data'
} as TableOptions)
class User extends Model {
	@AutoIncrement
	@PrimaryKey
	@Column(DataType.INTEGER)
	override id: number;

	@AllowNull(false)
	@Length({ max: 20, min: 3 })
	@Column(DataType.TEXT)
	first_name: string;

	@AllowNull(false)
	@Column(DataType.TEXT)
	theme: string;
}

export default User;