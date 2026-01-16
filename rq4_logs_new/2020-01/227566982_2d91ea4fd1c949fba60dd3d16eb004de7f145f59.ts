import { createSelectorEffectContext } from 'magic-selectors'
import { AppDispatch } from 'app/store'

export const {
  SelectorEffectContextProvider,
  addSelectorEffect,
  useMagicSelector: useSelector,
  makeNamedSelectorEffect,
  makeParameterizedSelectorEffect
} = createSelectorEffectContext<AppDispatch>()