import Hull3 = require('./Hull3');

export enum Side {
    BLUFOR,
    OPFOR,
    INDFOR,
    CIVILIAN
}

export enum MissionType {
    COOP,
    TVT,
    GTVT,
    COTVT
}

export interface Terrain {
    id: string;
    name: string;
}

export interface Faction {
    side: Side;
    faction: Hull3.Faction;
    gearTemplateId: string;
    uniformTemplateId: string;
}

export interface Addons {
    Admiral: boolean;
    Plank: boolean;
}

export interface Mission {
    terrain: Terrain;
    missionTypeName: string;
    maxPlayers: number;
    onLoadName: string;
    author: string;
    briefingName: string;
    overviewText: string;
    factions: Faction[];
    addons: Addons;
}

export interface Hull3Config {
    factions: Hull3.Faction[];
    gearTemplates: Hull3.Template[];
    uniformTemplates: Hull3.Template[];
}

export interface MissionConfig {
    missionTypeNames: string[];
    terrains: Terrain[];
    Hull3: Hull3Config;
}

export interface GeneratedMission {
    missionId: number;
    missionWorkingDir: string;
    missionDirName: string;
    missionDir: string;
}