import { Section } from './section.model';

export interface MapLocation {
    id: string;
    name: string;
    zoom: number;
    longitude: number;
    latitude: number;
    sections: Section[];
}