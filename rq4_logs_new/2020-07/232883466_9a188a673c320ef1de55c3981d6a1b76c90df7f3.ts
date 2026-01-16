import { getConfigWidok } from './config'
import { Context, contexts, ContextSymbol } from './Context'
import { managementWrapper, setupWrapper } from './contructors'
import { useStoreDevtools } from './devtools'

export function useStore<T>(name: string, s: Function, m: Function): () => T {
	return () => {
		let context = contexts.get(name)

		if (!context) {
			const context = Context.init(name)
			const state = setupWrapper(s())
			const management = managementWrapper(m(state))
			const instance = Object.assign(
				{
					[ContextSymbol]: context
				}, state, management
			)

			contexts.set(name, context)
			getConfigWidok().dev && useStoreDevtools(context)
			Context.setup(instance)

			return instance
		}

		return context.instance
	}
}