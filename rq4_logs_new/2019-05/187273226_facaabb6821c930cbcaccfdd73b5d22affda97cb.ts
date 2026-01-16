import { BaseMapTile } from '../models/base-map-tile.model';

export const baseMapTiles: BaseMapTile[] = [
  {
    key: 'alabeltile',
    name: 'Administrative Labels',
    description: 'A tile that contains only administrative labels.'
  },
  {
    key: 'basetile',
    name: 'Base Map Tile',
    description:
      'A base map tile, which is an image that shows the map and streets, but no labels.'
  },
  {
    key: 'blinetile',
    name: 'Base Line Map Tile',
    description: 'A base map tile without buildings, house numbers and labels.'
  },
  {
    key: 'labeltile',
    name: 'Label Tile',
    description: 'A tile that contains only labels.'
  },
  {
    key: 'linetile',
    name: 'Line Tile',
    description:
      'A line tile image, which contains only street lines, no labels.'
  },
  {
    key: 'lltile',
    name: 'Lines and Labels Tile',
    description:
      'A tile image, which shows street lines, city center labels and item labels, but no street labels.'
  },
  {
    key: 'streettile',
    name: 'Street Tile',
    description: 'An image that contains only street lines and labels.'
  }
];