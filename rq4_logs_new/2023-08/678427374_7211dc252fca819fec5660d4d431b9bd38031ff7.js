const ApiClient = require("../client/ApiClient");
const Client = require("../client/Client");
const Achievement = require("../structures/Achievement");
const Clan = require("../structures/Clan");
const ClanMember = require("../structures/ClanMember");
const Player = require("../structures/Player");
const PlayerAchievement = require("../structures/PlayerAchievement");
const PlayerAchievements = require("../structures/PlayerAchievements");
const PlayerClanData = require("../structures/PlayerClanData");
const SearchedClan = require("../structures/SearchedClan");
const SearchedPlayer = require("../structures/SearchedPlayer");
const Tank = require("../structures/Tank");
const TankStats = require("../structures/TankStats");

class ApiMethod {
    constructor(client, api) {
        /**
         * @type {ApiClient}
         */
        this.api = api;
        /**
         * @type {Client}
         */
        this.client = client;
    }
        /**
     * Search Players.
     * @param {string} search - Search.
     * @param {object} options - Options.
     */
    async searchPlayers(search, options=null) {
        if(search.constructor.name == 'Array') {
            search = search.join(',');
        }
        let path = `account/list/?application_id=${this.client.token}&search=${search}`;
        let realm = this.client.realm;
        if(options != null) {
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.language) path += `&language=${options.language}`;
            if(options.limit) path += `&limit=${options.limit}`;
            if(options.type) path += `&type=${options.type}`;
            if(options.realm) realm = options.realm;
        }
        let data = await this.api.requests.get(path, realm);
        let array = [];
        for(let a in data.data) {
            array.push(new SearchedPlayer(this.client, data.data[a], realm));
        }
        return array;
    }

    async getPlayer(id, realm, options=null) {
        let path = `account/info/?application_id=${this.client.token}&account_id=${id}`;
        if(options != null) {
            if(options.accessToken) path += `&access_token=${options.accessToken}`;
            if(options.extra) path += `&extra=${options.extra}`;
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.language) path += `&language=${options.language}`;
        }
        let data = await this.api.requests.get(path, realm);
        if(data.data[id] == null) return null;
        else return new Player(this.client, data.data[id], realm);
    }

    async getPlayerPrivateDataContacts(contacts, realm) {
        let ungr = [];
        for(let a in contacts.ungrouped) {
            ungr[a] = await this.getPlayer(contacts.ungrouped[a], realm);
        }
        let blck = [];
        for(let a in contacts.blocked) {
            blck[a] = await this.getPlayer(contacts.blocked[a], realm);
        }
        let groupedContacts = {
            ungrouped: ungr,
            blocked: blck,
        }
        return groupedContacts;
    }
    async getAchievments(options=null) {
        let path = `encyclopedia/achievements/?application_id=${this.client.token}`;
        let realm = this.client.realm;
        if(options != null) {
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.language) path += `&language=${options.language}`;
            if(options.realm) realm = options.realm;
        }
        let data = await this.api.requests.get(path, realm);
        return data.data;
    }
    async getPlayerAchievments(id, realm, options=null) {
        let path = `account/achievements/?application_id=${this.client.token}&account_id=${id}`;
        if(options != null) {
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.language) path += `&language=${options.language}`;
        }
        let data = await this.api.requests.get(path, realm);
        let allach = await this.getAchievments({realm: realm});
        let allAchievements = {};
        for(let a in allach) {
            allAchievements[a] = new Achievement(allach[a]);
        }
        let achievements = new Map();
        for(let a in data.data[id].achievements) {
            achievements.set(a, new PlayerAchievement(allAchievements[a], data.data[id].achievements[a]));
        }
        let maxSeries = new Map();
        for(let a in data.data[id].max_series) {
            maxSeries.set(a, new PlayerAchievement(allAchievements[a], data.data[id].max_series[a]));
        }
        return new PlayerAchievements(achievements, maxSeries);
    }

    async getPlayerClanData(id, realm, options=null) {
        let path = `clans/accountinfo/?application_id=${this.client.token}&account_id=${id}`;
        if(options != null) {
            if(options.extra) path += `&extra=${options.extra}`;
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.language) path += `&language=${options.language}`;
        }
        let data = await this.api.requests.get(path, realm);
        return new PlayerClanData(data.data[id]);
    }

    async searchClans(options=null) {
        let path = `clans/list/?application_id=${this.client.token}`;
        let realm = this.client.realm;
        if(options != null) {
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.language) path += `&language=${options.language}`;
            if(options.limit) path += `&limit=${options.limit}`;
            if(options.pageNo) path += `&page_no=${options.pageNo}`;
            if(options.search) path += `&search=${options.search}`;
            if(options.realm) realm = options.realm;
        }
        let data = await this.api.requests.get(path, realm);
        let array = [];
        for(let a in data.data) {
            array.push(new SearchedClan(this.client, data.data[a], realm));
        }
        return array;
    }

    async getClan(id, realm, options=null) {
        let path = `clans/info/?application_id=${this.client.token}&clan_id=${id}`
        if(options != null) {
            if(options.extra) path += `&extra=${options.extra}`;
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.language) path += `&language=${options.language}`;
        }
        let data = await this.api.requests.get(path, realm);
        return new Clan(this.client, data.data[id], realm);
    }
    
    getClanMembers(data, realm) {
        let array = [];
        for(let a in data) {
            array.push(new ClanMember(this.client, data[a], realm));
        }
        return array;
    }

    async getTanks(options=null) {
        let path = `encyclopedia/vehicles/?application_id=${this.client.token}`;
        let realm = this.client.realm;
        if(options != null) {
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.language) path += `&language=${options.language}`;
            if(options.nation) path += `&nation=${options.nation}`;
            if(options.tankID) path += `&tank_id=${options.tankID}`;
            if(options.realm) realm = options.realm;
        }

        let data = await this.api.requests.get(path, realm);
        let array = []
        for(let i in data.data) {
            if(data.data[i] != null)
                array.push(new Tank(data.data[i]))
        }
        return array;
    }

    async getTankStats(id, realm, options=null) {
        let path = `tanks/stats/?application_id=${this.client.token}&account_id=${id}`;
        if(options != null) {
            if(options.accessToken) path += `&access_token=${options.accessToken}`;
            if(options.fields) path += `&fields=${options.fields}`;
            if(options.inGarage) path += `&in_garage=${options.inGarage}`;
            if(options.language) path += `&language=${options.language}`;
            if(options.tankID) path += `&tank_id=${options.tankID}`;
        }
        let data = await this.api.requests.get(path, realm);
        let array = [];
        for(let a in data.data[id]) {
            array.push(new TankStats(this.client, realm, data.data[id][a]));
        }
        return array;
    }
}

module.exports = ApiMethod;