
interface IStream {
    id: number;
    game: string;
    viewers: number;
    preview: string;
    displayName: string;
    logo: string;
    statusMessage: string;
    url: string;
    followers: number;
    views: number;
}

export = IStream;