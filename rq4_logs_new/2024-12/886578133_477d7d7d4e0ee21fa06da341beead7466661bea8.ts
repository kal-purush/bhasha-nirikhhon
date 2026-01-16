// @ts-ignore: this is safe, we don't want to actually make renderexcalidraw.inline.ts a module as
// modules are automatically deferred and we don't want that to happen for critical beforeDOMLoads
// see: https://v8.dev/features/modules#defer
import script from "./scripts/renderexcalidraw.inline";
import { QuartzComponent, QuartzComponentConstructor } from "./types"

export default (() => {
  const RenderExcalidraw = () => {}
  RenderExcalidraw.afterDOMLoaded = script

  return RenderExcalidraw
}) satisfies QuartzComponentConstructor