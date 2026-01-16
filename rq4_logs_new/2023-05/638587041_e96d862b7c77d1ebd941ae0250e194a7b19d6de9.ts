import { ConnectedArtistObject } from "../../models/Artist";

export interface GetSeveralAlbumsResponse {
  albums: Album[];
}

export interface Album {
  album_type: string;
  total_tracks: number;
  available_markets: string[];
  external_urls: ExternalUrls;
  href: string;
  id: string;
  images: Image[];
  name: string;
  release_date: string;
  release_date_precision: string;
  restrictions: Restrictions;
  type: string;
  uri: string;
  copyrights: Copyright[];
  external_ids: ExternalIDS;
  genres: string[];
  label: string;
  popularity: number;
  artists: Artist[];
  tracks: Tracks;
}

export interface Artist {
  external_urls: ExternalUrls;
  followers: Followers;
  genres: string[];
  href: string;
  id: string;
  images: Image[];
  name: string;
  popularity: number;
  type: string;
  uri: string;
}

export interface ExternalUrls {
  spotify: string;
}

export interface Followers {
  href: string;
  total: number;
}

export interface Image {
  url: string;
  height: number;
  width: number;
}

export interface Copyright {
  text: string;
  type: string;
}

export interface ExternalIDS {
  isrc: string;
  ean: string;
  upc: string;
}

export interface Restrictions {
  reason: string;
}

export interface Tracks {
  href: string;
  limit: number;
  next: string;
  offset: number;
  previous: string;
  total: number;
  items: Item[];
}

export interface Item {
  artists: LinkedFrom[];
  available_markets: string[];
  disc_number: number;
  duration_ms: number;
  explicit: boolean;
  external_urls: ExternalUrls;
  href: string;
  id: string;
  is_playable: boolean;
  linked_from: LinkedFrom;
  restrictions: Restrictions;
  name: string;
  preview_url: string;
  track_number: number;
  type: string;
  uri: string;
  is_local: boolean;
}

export interface LinkedFrom {
  external_urls: ExternalUrls;
  href: string;
  id: string;
  name?: string;
  type: string;
  uri: string;
}

export const mapAlbumsToConnectedArtists = (
  albums: Album[]
): ConnectedArtistObject[] => {
  return albums.map((album) => {
    const connectedArtist: ConnectedArtistObject = {
      trackName: album.name,
      artists: album.artists.flatMap((artist) => {
        return {
          name: artist.name,
          image: "",
          id: artist.id,
          href: artist.href,
        };
      }),
      id: album.id,
      image: album.images[0].url,
      sampleTrackURL: album.tracks.items[0].preview_url,
      href: album.href,
      tracks: album.tracks,
    };

    return connectedArtist;
  });
};