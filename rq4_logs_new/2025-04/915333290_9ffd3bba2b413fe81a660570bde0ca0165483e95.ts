export const fetchLocations = async (query: string) => {
  if (!query) return [];

  try {
    const cityUrl = `https://photon.komoot.io/api/?q=${query}&limit=5&osm_tag=place:city`;
    let response = await fetch(cityUrl);
    let data = await response.json();

    if (data.features.length > 0) return data.features.map(formatSuggestion);

    const generalUrl = `https://photon.komoot.io/api/?q=${query}&limit=5`;
    response = await fetch(generalUrl);
    data = await response.json();

    return data.features.map(formatSuggestion);
  } catch (error) {
    console.error("Error fetching locations:", error);
    return [];
  }
};

const formatSuggestion = (feature: any) => {
  return {
    name: feature.properties.name,
    country: feature.properties.country,
    type: feature.properties.osm_value,
  };
};