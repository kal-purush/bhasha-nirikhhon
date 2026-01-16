import { useQuery } from "$hooks";
import { GET_JOB_APPLICATION_BY_UUID } from "$queries";
import { RoutesBuilder } from "$models/RoutesBuilder";
import { useHistory } from "react-router-dom";
import { IJobApplication } from "$interfaces/JobApplication";

export const useJobApplicationByUuid = (uuid: string) => {
  const history = useHistory();
  return useQuery<{}, { getJobApplicationByUuid: IJobApplication }>(
    GET_JOB_APPLICATION_BY_UUID,
    {
      variables: { uuid },
      errorHandlers: {
        JobApplicationNotFoundError: () => history.push(RoutesBuilder.public.notFound()),
        defaultHandler: () => history.push(RoutesBuilder.public.internalServerError())
      }
    }
  );
};