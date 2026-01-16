import axios, { AxiosResponse, AxiosError } from "axios";
import getHeaderTokenAndDB from "../../../../helpers/getHeaderTokenAndDB";
import { ServiceErrorHandler } from "../../../../helpers/ServiceErrorHandler";

const getMoraXOficial = async (data) => {
  try {
    const headers = getHeaderTokenAndDB();
    const response: AxiosResponse = await axios.post(
      process.env.REACT_APP_HOST + "Reportes/MoraXOficial",
      data,
      headers
    );
    if (
      response.data.hasOwnProperty("resumen") &&
      response.data.hasOwnProperty("detalle")
    ) {
      return response.data;
    } else {
      throw response.data;
    }
  } catch (error) {
    return ServiceErrorHandler(error, "Reportes/MoraXOficial");
  }
};

const MoraXOficialService = {
  getMoraXOficial,
};

export default MoraXOficialService;