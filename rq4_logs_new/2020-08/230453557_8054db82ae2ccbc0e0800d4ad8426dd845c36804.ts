import { useQuery } from "../useQuery";
import { GET_MY_JOB_APPLICATIONS } from "$queries";
import { RoutesBuilder } from "../../RoutesBuilder";
import { IJobApplication } from "$interfaces/JobApplication";
import { useHistory } from "react-router-dom";
import { IPaginatedResult } from "./interface";
import { FetchResult } from "$interfaces/Pagination";

export const useMyJobApplications = () => {
  const history = useHistory();
  const result = useQuery<{}, IUseMyJobApplications>(
    GET_MY_JOB_APPLICATIONS,
    {
      errorHandlers: {
        UnauthorizedError: () => history.push(RoutesBuilder.public.forbidden())
      }
    }
  );
  const fetchMore = () => {
    const applications = result.data?.getMyLatestJobApplications.results;
    if (!applications) return;
    const lastApplication = applications[applications.length - 1];
    return result.fetchMore({
      query: GET_MY_JOB_APPLICATIONS,
      variables: {
        updatedBeforeThan: {
          dateTime: lastApplication.updatedAt,
          offerUuid: lastApplication.offer.uuid,
          applicantUuid: lastApplication.applicant.uuid
        }
      }
    }) as FetchResult<IUseMyJobApplications>;
  };
  return { ...result, fetchMore };
};

export interface IUseMyJobApplications {
  getMyLatestJobApplications: IPaginatedResult<IJobApplication>;
}