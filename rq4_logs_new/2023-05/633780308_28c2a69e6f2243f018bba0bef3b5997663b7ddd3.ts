import { useState, useCallback } from "react";
import { LocationT } from "../types/types";

import axios, { AxiosError } from "axios";

type ReturnType = {
  isLoading: boolean;
  isError: boolean;
  errorMsg: string;
  resetError: () => void;
  sendRequest: (method: "GET", url: string, fn: CallBackFunction) => void;
};

// This needs refactoring (need a better way to reuse types)
type ResponseT<T> = {
  locations: Array<T>;
};

type CallBackFunction = (data: ResponseT<LocationT>) => void;

const useBathingWaterRequest = (): ReturnType => {
  const [isLoading, setIsLoading] = useState(false);
  const [isError, setIsError] = useState(false);
  const [errorMsg, setErrorMsg] = useState("");

  const sendRequest = useCallback(
    async (method: "GET", url: string, fn: CallBackFunction): Promise<void> => {
      setIsLoading(true);
      setIsError(false);
      try {
        let data;

        if (method === "GET") {
          data = await axios.get(`${url}`);
        }

        if (!data) {
          setIsLoading(false);
          throw data;
        }

        const formattedResults: Array<LocationT> = [];

        const result = data.data.result.items;

        for (const item of result) {
          const locationData = {} as LocationT;

          locationData.latestAssessmentGrade =
            item.latestComplianceAssessment.complianceClassification.name._value;

          locationData.location = item.name._value;

          locationData.lat = item.samplingPoint.lat;
          locationData.long = item.samplingPoint.long;

          locationData.nameOfWaterCompany =
            item.appointedSewerageUndertaker.name._value;

          locationData.id = item.eubwidNotation;

          fetch(
            `https://environment.data.gov.uk/data/bathing-water-profile/${item.eubwidNotation}/2023:1.json`
          ).then((data) => {
            data.json().then((data) => {
              if (!data) {
                setIsLoading(false);
                throw data;
              }

              const result = data.result;

              locationData.yearOfAssessmentStart =
                result.primaryTopic.seasonStartDate._value;

              locationData.yearOfAssessmentFinish =
                result.primaryTopic.seasonFinishDate._value;

              locationData.locationDescription =
                result.primaryTopic.zoiDescription._value;

              formattedResults.push(locationData);
            });
          });
        }

        const finalResult = {
          locations: formattedResults,
        };

        fn(finalResult);
      } catch (err: unknown) {
        setIsError(true);
        if (err instanceof AxiosError) {
          setErrorMsg(
            err.message || "Oops, something went wrong. Please try again!"
          );
        }
      }

      setIsLoading(false);
    },
    []
  );

  const resetError = () => {
    setIsError(false);
  };

  const request = {
    isLoading,
    isError,
    errorMsg,
    resetError,
    sendRequest,
  };

  return request;
};

export default useBathingWaterRequest;