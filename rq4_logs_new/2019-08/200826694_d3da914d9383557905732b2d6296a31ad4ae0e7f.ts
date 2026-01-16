import { songList } from './SongList'

export class ChooseSong
{
    constructor() {}

    public chooseSong(): string {
        let random = Math.floor(Math.random() * Object.keys(songList).length)
        return songList[random]
    }
}