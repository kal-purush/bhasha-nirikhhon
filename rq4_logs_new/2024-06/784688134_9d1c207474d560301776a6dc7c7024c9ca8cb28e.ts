export type RestApiContext = {
  id: string;
  allocation: any;
  type: string;
  name: string;
  template: string;
  thumbnail: string;
  thumbnail_full_size: string;
  origin: RestApiOrigin;
  description: { [key: string]: string };
  parents: string[];
  localDepth: number;
  item: [];
  context: any[];
  content: any[];
};

export type RestApiOrigin = {
  applications: any[];
  server: string[];
  authors: any[];
  created: number;
};