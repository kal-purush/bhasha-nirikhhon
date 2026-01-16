import { DataTypes, Model } from 'sequelize';

import database from '..';

import { Movies } from './Movies';

export interface TranslationsAttributes {
    id: string;

    originalId: string;

    name: string;

    englishName: string;

    overview: string;

    title: string;

    movie: string;
}

export class Translations extends Model implements TranslationsAttributes {
    id: string;

    originalId: string;

    name: string;

    englishName: string;

    overview: string;

    title: string;

    movie: string;
}

Translations.init({
  id: {
    primaryKey: true,
    type: DataTypes.STRING,
    allowNull: false,
  },
  originalId: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  name: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  englishName: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  ovreview: DataTypes.STRING,
  title: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  user: {
    type: DataTypes.STRING,
    allowNull: false,
  },
}, {
  sequelize: database.connection,
  modelName: 'Movies',
});

Movies.hasMany(Translations, {
  foreignKey: 'movie',
  sourceKey: 'id',
});

Translations.belongsTo(Movies, {
  foreignKey: 'movie',
  targetKey: 'id',
});