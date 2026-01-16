type RawLaunch = {
  name: string;
  date_unix: number;
  flight_number: number;
  details: string | null;
  rocket: string | null;
  launchpad: string | null;
  links: RawPatchLinks;
  success: boolean | null;
  cores: RawCores[];
};

type RawPatchLinks = {
  patch: {
    small: string | null;
    large: string | null;
  };
};

type RawCores = {
  landing_attempt: boolean | null;
  landing_success: boolean | null;
};

type RawRocket = {
  name: string;
};

type RawLaunchpad = {
  region: string;
};

export type { RawLaunch, RawRocket, RawLaunchpad };