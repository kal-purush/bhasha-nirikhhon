import { waitListApi } from "./waitlist-api";
import { WaitlistData } from "<@>/types/admin/wait-list-data-response";
import { GetWaitlistParams } from "<@>/types/admin/wait-list-search-params";

const applicantApi = waitListApi.injectEndpoints({
    endpoints(builder) {
        return {
          applicantsWaitlist: builder.query<WaitlistData, GetWaitlistParams>({
            query: (params) => ({
              url: "/Waitlist",
              params,
            }),
            providesTags: ["Waitlist"],
          }),
        };
      },
    overrideExisting: false,
  });

  export const { useApplicantsWaitlistQuery } = applicantApi;