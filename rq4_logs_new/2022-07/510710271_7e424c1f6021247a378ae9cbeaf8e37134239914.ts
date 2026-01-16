import * as queryString from 'query-string';
import * as TE from 'fp-ts/lib/TaskEither';
import * as E from 'fp-ts/lib/Either';
import * as T from 'fp-ts/lib/Task';
import * as t from 'io-ts';
import { pipe } from 'fp-ts/lib/function';

export type Client = {
  id: string;
  secret: string;
  redirectUri: string;
};

export type GithubToken = {
  accessToken: string,
  tokenType: string,
  scopes: string
};

const GithubTokenCodec = t.type({
  accessToken: t.string,
  tokenType: t.string,
  scopes: t.string,
});

const parseQueryString = <P>(codec: t.Type<P>) => (x: string): E.Either<unknown, P>  => {
  const parsed = queryString.parse(x);
  return pipe(parsed.error ? E.left(parsed) : E.right(parsed),
    E.chain(codec.decode));
}

export const getAccessToken = (client: Client) => (code: string): TE.TaskEither<unknown, GithubToken>  => {
  const params = queryString.stringify({
    client_id: client.id,
    client_secret: client.secret,
    redirect_uri: client.redirectUri,
    code,
  });
  return pipe(
    TE.tryCatch(() => fetch(`https://github.com/login/oauth/access_token?${params}`), (err: unknown) => err),
    TE.chainTaskK(resp => resp.text),
    TE.chainEitherK(parseQueryString(GithubTokenCodec)))
}