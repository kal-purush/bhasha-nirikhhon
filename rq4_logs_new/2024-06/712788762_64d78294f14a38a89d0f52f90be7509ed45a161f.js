import { getData } from "./fetchData";
export async function fetchUrlParams() {
  try {
    const articlesData = await getData("articles");
    const { articles } = articlesData;

    const doctorsData = await getData("doctors");
    const { doctors } = doctorsData;

    const servicesData = await getData("services");
    const { services } = servicesData;

    const subServicesData = await getData("subservices");
    const { subServices } = subServicesData;

    const subServiceDirectionsData = await getData(
      "subservice-direction/subdirection"
    );
    const { subServiceDirections } = subServiceDirectionsData;

    return {
      articles,
      doctors,
      services,
      subServices,
      subServiceDirections,
    };
  } catch (error) {
    console.error("Error fetching data:", error);
    throw error;
  }
}