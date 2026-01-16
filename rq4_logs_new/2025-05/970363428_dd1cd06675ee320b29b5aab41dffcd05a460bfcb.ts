import { useEffect } from "react"

/**
 * Add the given classes to the body and/or html element.
 *
 * Restores the original classes at cleanp.
 *
 * @param bodyClasses - classes to assign to the body element
 * @param htmlClasses - classes to assign to the html element
 *
 * @example
 * ```tsx
 * import { useBodyClass } from "@trashpanda001/helpers/react"
 *
 * function Component() {
 *   useBodyClass("bg-gray-800, "bg-gray-900")
 *   return <div>...</div>
 * }
 * ```
 */
export function useBodyClass(bodyClasses: string, htmlClasses: string = "") {
  useEffect(() => {
    const oldBodyClasses = document.body.className
    const oldHtmlClasses = document.documentElement.className
    if (bodyClasses) {
      document.body.className += " " + bodyClasses
    }
    if (htmlClasses) {
      document.documentElement.className += " " + htmlClasses
    }
    return () => {
      document.body.className = oldBodyClasses
      document.documentElement.className = oldHtmlClasses
    }
  }, [bodyClasses, htmlClasses])
}