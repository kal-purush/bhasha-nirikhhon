import {
  getQueryParam,
  putQueryParam,
  toUrl,
  URLParser,
} from "@execonline-inc/url";
import { always } from "@kofno/piper";
import Decoder from "jsonous";
import { fromNullable, just, Maybe } from "maybeasy";
import Task from "taskarian";

export interface CouldNotCreateChildError {
  kind: "could-not-create-child-error";
  url: string;
}

const noop = () => {};

const whenEmailParamPresent = (url: URLParser): Maybe<URLParser> =>
  just(url)
    .andThen(getQueryParam("email"))
    .andThen(fromNullable)
    .map(always(url));

const anonymizeEmailParam = (urlString: string): string =>
  toUrl(urlString)
    .andThen(whenEmailParamPresent)
    .map(putQueryParam("email", "FILTERED"))
    .map((url) => url.href)
    .getOrElseValue(urlString);

const couldNotCreateChildError = (url: string): CouldNotCreateChildError => ({
  kind: "could-not-create-child-error",
  url: anonymizeEmailParam(url),
});

export const withChild = (
  url: string
): Task<CouldNotCreateChildError, Window> =>
  new Task((reject, resolve) => {
    const child = window.open(url);
    if (child) {
      resolve(child);
    } else {
      reject(couldNotCreateChildError(url));
    }
    return noop;
  });

export interface DidNotConfirm {
  kind: "did-not-confirm";
}

export interface DidConfirm {
  kind: "did-confirm";
}

const didNotConfirm = (): DidNotConfirm => ({ kind: "did-not-confirm" });

const didConfirm = (): DidConfirm => ({ kind: "did-confirm" });

export const confirmT = (msg: string): Task<DidNotConfirm, DidConfirm> =>
  new Task((reject, resolve) => {
    window.confirm(msg) ? resolve(didConfirm()) : reject(didNotConfirm());
    return noop;
  });

export interface CouldNotReloadError {
  kind: "could-not-reload-error";
}

const couldNotReloadError = (): CouldNotReloadError => ({
  kind: "could-not-reload-error",
});

export const locationReload = (): Task<CouldNotReloadError, void> =>
  new Task((reject, resolve) => {
    try {
      window.location.reload();
    } catch {
      reject(couldNotReloadError());
      return noop;
    }
    resolve();
    return noop;
  });

export const fromDecoderAny =
  <T>(decoder: Decoder<T>) =>
  (value: unknown): Task<string, T> =>
    new Task((reject, resolve) => {
      decoder.decodeAny(value).cata({ Ok: resolve, Err: reject });
      return noop;
    });

export const fromDecoderJson =
  <T>(decoder: Decoder<T>) =>
  (value: string): Task<string, T> =>
    new Task((reject, resolve) => {
      decoder.decodeJson(value).cata({ Ok: resolve, Err: reject });
      return noop;
    });