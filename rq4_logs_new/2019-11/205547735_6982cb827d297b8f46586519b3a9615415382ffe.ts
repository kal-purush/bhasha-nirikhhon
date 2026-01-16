import {Option, some, fold} from "fp-ts/lib/Option"
import {TaskEither, taskEither} from "fp-ts/lib/TaskEither"
import {AccountResult} from "./types"

export const makeString = (arg: any) => String(arg)

export const trace = <X>(arg: X) => {
  console.log(makeString(arg))
  return arg
}

export const err = (e: Error) => {
  throw e
}

export const someNum: (num: number) => Option<number> = some
export const emptyTask = (): TaskEither<Error, number> => taskEither.of(-1)

export const getId = (accountResult: AccountResult) => accountResult.accountId

export const fold2 = <X, Y>(func0: () => Y, func: (a1: X, a2: X) => Y) => (
  arg1: Option<X>,
  arg2: Option<X>
) =>
  fold(func0, (unwrapped2: X) =>
    fold(func0, (unwrapped1: X) => func(unwrapped1, unwrapped2))(arg1)
  )(arg2)