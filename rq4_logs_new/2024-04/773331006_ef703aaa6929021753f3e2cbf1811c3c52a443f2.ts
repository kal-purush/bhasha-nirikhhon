export class PlanetsApi {
  private apiBase = 'https://swapi.dev/api';

  async getResource(url: string) {
    const res = await fetch(`${this.apiBase}${url}`);
    if (!res.ok) {
      throw new Error(`Could not fetch ${url}, received ${res.status}`);
    }
    return res.json();
  }

  async getAllPlanet() {
    return this.getResource(`/planets`);
  }

  async getPlanets(id: number | string) {
    return this.getResource(`/planets/${id}`);
  }
}

/* const Data = new GetData();
Data.getAllPlanet().then((body) => {
  console.log(body);
}); */