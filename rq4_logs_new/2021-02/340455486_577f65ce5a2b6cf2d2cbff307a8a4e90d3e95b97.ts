import {pipe} from '@effect-ts/core/Function';
import {getA, LiveLayerA} from './src/layer-a';
import * as R from '@effect-ts/node/Runtime';
import * as T from '@effect-ts/core/Effect';

pipe(
    pipe(
        getA,
        T.tap((a) => T.effectTotal(() => console.log(a)))
    ),
    T.provideSomeLayer(
        LiveLayerA,
    ),
    T.result,
    T.chain(T.done),
    R.runMain,
);