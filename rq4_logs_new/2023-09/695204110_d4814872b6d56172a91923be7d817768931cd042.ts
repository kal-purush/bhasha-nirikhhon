export type AccountantType = {
  // W tych czasach typować gender jako unię jest niebezpiecznie ( ͡° ͜ʖ ͡°)
  gender: string;

  cell: string;
  name: {
    first: string;
    last: string;
  };
  email: string;
  picture: { thumbnail: string; medium: string };
  login: {
    uuid: string;
  };
};

export type AccountantResponseType = {
  results: AccountantType[];
  info: {
    page: number;
  };
};