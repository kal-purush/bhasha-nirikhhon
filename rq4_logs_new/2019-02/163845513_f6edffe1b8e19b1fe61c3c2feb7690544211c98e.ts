import * as Factory from "factory.ts"
import aguid from "aguid"

/* prettier breaks this code somehow */
/* TODO: investigate */
// prettier-ignore
export type Journal = {
  id: string,
  name: string,
}

export const journalFactory = Factory.Sync.makeFactory<Journal>({
  id: Factory.each(i => aguid(`${i}`)),
  name: "create oss pr",
})