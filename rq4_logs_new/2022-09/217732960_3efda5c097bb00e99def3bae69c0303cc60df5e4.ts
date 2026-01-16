import { render } from '@testing-library/svelte'
import ContextComp from './Context.svelte'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Context<T = any> = [string, T];

export function withContext(context: Context) {
  return (...[component, props, ...args]: Parameters<typeof render>) =>
    render(ContextComp, {props: { component, context, ...props }}, ...args)
}