interface IRegionRestriction {
    allowed: string[];
    blocked: string[];
}

export interface IYouTubeContentDetails {
    caption?: string;
    definition?: string;
    dimension?: string;
    duration?: string;
    licensedContent?: string;
    regionRestriction?: IRegionRestriction;
}


export class YouTubeContentDetails {
    caption: string;
    definition: string;
    dimension: string;
    duration: string;
    licensedContent: string;
    regionRestriction: IRegionRestriction;

    constructor(data: IYouTubeContentDetails) {
        this.caption = data.caption;
        this.definition = data.definition;
        this.dimension = data.dimension;
        this.duration = data.duration;
        this.licensedContent = data.licensedContent;
        this.regionRestriction = data.regionRestriction
    }
}