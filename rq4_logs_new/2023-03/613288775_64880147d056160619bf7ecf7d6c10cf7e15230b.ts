import { z } from "zod";
import { env } from "~/env.mjs";
import { devOrProd } from ".";

const MovieInfo = z.object({
  id: z.number(),
  title: z.string(),
  overview: z.string().nullable(),
  popularity: z.number(),
  runtime: z.number().nullable(),
  release_date: z.string(),
  vote_average: z.number(),
  poster_path: z
    .string()
    .nullable()
    .transform(path => `https://image.tmdb.org/t/p/original${path}`),
  backdrop_path: z
    .string()
    .nullable()
    .transform(path => `https://image.tmdb.org/t/p/original${path}`),
  genres: z.array(
    z.object({
      name: z.string(),
    }),
  ),
});

export type MovieType = z.infer<typeof MovieInfo>;

export default async function getMovie(id: string) {
  const data = await await fetch(
    `https://api.themoviedb.org/3/movie/${id}?api_key=${env.NEXT_PUBLIC_API_KEY}&language=en-US`,
  ).then(res => res.json());

  const results = MovieInfo.parse(data);

  return results;
}