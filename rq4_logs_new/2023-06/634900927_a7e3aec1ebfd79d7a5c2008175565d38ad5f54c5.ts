export type SponsorList = {
  contents: Sponsor[];
};

export type Sponsor = {
  id: string;
  createdAt: string;
  updatedAt: string;
  revisedAt: string;
  url: SponsorURL;
  twitter?: string;
  siteURL?: string;
};

export type SponsorURL = {
  url: string;
  height: number;
  width: number;
};