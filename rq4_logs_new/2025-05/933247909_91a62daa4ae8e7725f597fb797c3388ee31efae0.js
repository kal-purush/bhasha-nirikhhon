import fetch from "node-fetch";

// Search city function
const searchCity = async (req, res) => {
  const city = req.query.city;

  if (!city || typeof city !== "string" || city.length < 3) {
    return res.status(400).json({ error: "Parametro city non valido" });
  }

  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/search?format=json&addressdetails=1&limit=5&accept-language=it&countrycodes=it&city=${encodeURIComponent(
        city
      )}`
    );
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error("Errore nel proxy Nominatim:", error);
    res.status(500).json({ error: "Errore nel proxy Nominatim" });
  }
};

// Search on map function
const searchOnMap = async (req, res) => {
  const address = req.query.address;
  if (!address || typeof address !== "string") {
    return res.status(400).json({ error: "Parametro address non valido" });
  }

  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(
        address
      )}`
    );
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error("Errore nel proxy Nominatim:", error);
    res.status(500).json({ error: "Errore nel proxy Nominatim" });
  }
};
export { searchCity, searchOnMap };