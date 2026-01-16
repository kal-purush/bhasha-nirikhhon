// src/models/Role.ts
import { Model } from 'objection';

// Bikin class Role yang extend dari Model
class Role extends Model {
  // Nama tabel yang dipake model ini
  static tableName = 'roles';

  // Definisikan properti-properti model
  id!: number;
  role_name!: string;

  // Relasi ke model User
  static get relationMappings() {
    return {
      users: {
        relation: Model.HasManyRelation,  // Hubungan has-many
        modelClass: `${__dirname}/User`,  // Path ke model User
        join: {
          from: 'roles.id',
          to: 'users.role_id'
        }
      }
    };
  }
}

export default Role;