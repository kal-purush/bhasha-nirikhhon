import { customFetch } from '../../utils';

type QueryParams = {
  search?: string;
  genres?: string;
  developers?: string;
  creators?: string;
  platforms?: string;
  publishers?: string;
  page_size?: number;
  page?: number;
};

const allGamesQuery = (queryParams: QueryParams) => {
  const {
    search,
    genres,
    developers,
    creators,
    platforms,
    publishers,
    page_size,
    page,
  } = queryParams;

  return {
    queryKey: [
      'games',
      search ?? '',
      genres ?? '',
      developers ?? 'ubisoft',
      creators ?? '',
      platforms ?? '',
      publishers ?? '',
      page_size ?? 20,
      page ?? 1,
    ],
    queryFn: () =>
      customFetch({
        params: queryParams,
      }),
  };
};

type QueryClient = {
  ensureQueryData: (query: any) => Promise<any>;
};

type Request = {
  url: string;
};

type Game = {
  // define the properties of a game here
};

type ResponseData = {
  results: Game[];
  // meta: any; // uncomment and replace `any` with the actual type if needed
};

type LoaderResponse = {
  games: Game[];
  params: Record<string, string>;
};

export const gamesLoader =
  (queryClient: QueryClient) =>
  async ({ request }: { request: Request }): Promise<LoaderResponse> => {
    const entries = [...new URL(request.url).searchParams.entries()];
    const params = Object.fromEntries(
      entries.filter(([, value]) => value !== ''),
    );
    // Set default developer parameter if it's not present
    // if (!params.developers) {
    //   params = { ...params, developers: 'ubisoft' };
    // }

    const response = (await queryClient.ensureQueryData(
      allGamesQuery(params),
    )) as { data: ResponseData };
    console.log(response.data, params);
    const games = response.data.results;
    // const meta = response.data.meta; // uncomment if needed
    return { games, params };
  };