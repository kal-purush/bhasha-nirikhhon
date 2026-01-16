import {
  Arbitrary,
  Random,
  Shrinkable
} from "fast-check"
import { Fun } from "@famisoft/overture/data/function"
import { Unit } from "@famisoft/overture/data/unit"

import { SP } from "./proc"
import { nil } from "./nil"
import { get } from "./get"
import { put } from "./put"

class ProcArbitrary<A, B> extends Arbitrary<SP<A, B>> {
  constructor (
    readonly arb: Arbitrary<B>,
    readonly depth: number
  ) {
    super()
  }

  generate (mrng: Random): Shrinkable<SP<A, B>> {
    function gen(arb: Arbitrary<B>, depth: number, clonedMrng: Random): SP<A, B> {
      if (depth <= 0) return nil()

      const cont: SP<A, B> = SP.defer((
        _ => gen(arb, depth - 1, clonedMrng)
      ) as Fun<Unit, SP<A, B>>)

      if (clonedMrng.nextBoolean()) {
        return put(
          arb.generate(clonedMrng).value_,
          cont
        )
      } else {
        return get((_ => cont) as Fun<A, SP<A, B>>)
      }
    }

    return new Shrinkable(gen(this.arb, this.depth, mrng.clone()))
  }
}


export default function arbitraryProc<A, B> (
  arbB: Arbitrary<B>,
  depth: number
): Arbitrary<SP<A, B>> {
  return new ProcArbitrary(arbB, depth)
}