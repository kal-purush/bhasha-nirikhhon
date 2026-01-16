import ITrack from "../../interfaces/music/ITrack";

export default class Track implements ITrack {
    artistName: string;
    id: string;
    imageUrl: string;
    trackName: string;
    trackUrl: string;
}