type WeatherData = {
  departure: {
    temperature: number | undefined;
    status: string | undefined;
  };
  arrival: {
    temperature: number | undefined;
    status: string | undefined;
  };
};

export type { WeatherData };