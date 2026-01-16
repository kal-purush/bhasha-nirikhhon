import * as Sequelize from 'sequelize';
require('dotenv').config();

export class Store
{
    private static _instance: Store;
    private _location: any;

    private constructor()
    {
    }

    public static getInstance(): Store
    {
        if(Store._instance === null || Store._instance === undefined)
        {
            Store._instance = new Store();
        }

        return Store._instance;
    }

    get location(): any
    {
        return this._location;
    }

    set location(location: any)
    {
        this._location = location;
    }
}