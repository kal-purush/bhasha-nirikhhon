export interface imageLinks {
	smallThumbnail: string;
	thumbnail: string;
	previewLink: string;
	infoLink: string;
	canonicalVolumeLink: string;
}

export interface IVolumeInfo {
	title: string;
	authors: string[];
	publisher: string;
	publishedDate: string;
	description: string;
	pageCount: number;
	categories: [];
	averageRating: number;
	ratingsCount: number;
	imageLinks: imageLinks;
}
export interface IVolume {
	id: string;
	volumeInfo: IVolumeInfo;
}

export interface IVolumes {
	[key: string]: IVolume[];
}

export interface IBookShelves {
	id: number;
	title: string;
	volumeCount: number;
}

export interface ILibrary {
	shelves: IBookShelves[];
	selecredShelves: string[];
}