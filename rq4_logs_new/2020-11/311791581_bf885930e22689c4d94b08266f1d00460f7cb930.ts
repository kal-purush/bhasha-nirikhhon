import { DataTypes, Model } from 'sequelize';

import database from '..';

export interface MoviesAttributes {
    id: string;
    title: string;
    overview: string;
}

export class Movies extends Model implements MoviesAttributes {
    id: string;

    title: string;

    overview: string;
}

Movies.init({
  id: {
    primaryKey: true,
    type: DataTypes.STRING,
    allowNull: false,
  },
  title: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  overview: DataTypes.TEXT,
}, {
  sequelize: database.connection,
  modelName: 'Movies',
});