import axios, { AxiosError, AxiosResponse } from "axios";
import getHeaderToken from "../../../helpers/getHeaderTokenAndDB";
import { getFunction, postFunction } from "../../Axios/axiosFunctions";
import { ServiceErrorHandler } from "../../../helpers/ServiceErrorHandler";

const getAdjudicaciones = async (data) => {
  try {
    //CONVERTIRLO EN FUNCION GENERICA PORQUE SE REPITE EL CASO EN VARIAS FUNCIONES DE LA APP
    const headers = getHeaderToken();
    const response: AxiosResponse = await axios.post(
      process.env.REACT_APP_HOST + "Reportes/efectividadAdj",
      data,
      headers
    );
    if (Array.isArray(response.data)) {
      return response.data;
    } else {
      throw response.data;
    }
  } catch (error) {
    return ServiceErrorHandler(error, "Reportes/efectividadAdj");
  }
};

const getOficialesAdj = async () => {
  return getFunction("Reportes/efectividadAdj/oficiales");
};

const efectividadAdjService = {
  getAdjudicaciones,
  getOficialesAdj,
};

export default efectividadAdjService;