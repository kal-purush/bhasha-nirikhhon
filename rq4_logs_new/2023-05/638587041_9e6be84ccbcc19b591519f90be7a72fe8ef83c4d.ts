import { SimpleArtistObject } from "../../models/Artist";

export interface FollowedArtistResponse {
  artists: Artists;
}

export interface Artists {
  href: string;
  next: string;
  total: number;
  items: Artist[];
}

export interface Artist {
  genres: string[];
  href: string;
  id: string;
  images: Image[];
  name: string;
  popularity: number;
  uri: string;
}

export interface Image {
  url: string;
  height: number;
  width: number;
}

export const mapToSimpleArtistObject = (
  artists: Artist[]
): SimpleArtistObject[] => {
  const followedArtists: SimpleArtistObject[] = artists.map(
    (artist: Artist) => {
      return {
        id: artist.id,
        name: artist.name,
        uri: artist.uri,
        image: artist.images[0].url,
        href: artist.href,
        popularity: artist.popularity,
        genres: artist.genres,
      };
    }
  );

  return followedArtists;
};