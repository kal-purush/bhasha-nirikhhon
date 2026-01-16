import { renderHook } from "@testing-library/react"
import { useBodyClass } from "@trashpanda001/helpers/react"
import { afterEach, beforeEach, describe, expect, it } from "vitest"

describe("useBodyClass", () => {
  let originalBodyClassName: string
  let originalHtmlClassName: string

  beforeEach(() => {
    originalBodyClassName = document.body.className
    originalHtmlClassName = document.documentElement.className
    document.body.className = ""
    document.documentElement.className = ""
  })

  afterEach(() => {
    document.body.className = originalBodyClassName
    document.documentElement.className = originalHtmlClassName
  })

  it("adds classes to body element", () => {
    const { unmount } = renderHook(() => useBodyClass("test-class"))

    expect(document.body.className.trim()).toBe("test-class")
    expect(document.documentElement.className.trim()).toBe("")

    unmount()

    expect(document.body.className.trim()).toBe("")
  })

  it("adds classes to both body and html elements", () => {
    const { unmount } = renderHook(() => useBodyClass("body-class", "html-class"))

    expect(document.body.className.trim()).toBe("body-class")
    expect(document.documentElement.className.trim()).toBe("html-class")

    unmount()

    expect(document.body.className.trim()).toBe("")
    expect(document.documentElement.className.trim()).toBe("")
  })

  it("preserves existing classes", () => {
    document.body.className = "existing-body"
    document.documentElement.className = "existing-html"

    const { unmount } = renderHook(() => useBodyClass("new-body", "new-html"))

    expect(document.body.className.trim()).toBe("existing-body new-body")
    expect(document.documentElement.className.trim()).toBe("existing-html new-html")

    unmount()

    expect(document.body.className.trim()).toBe("existing-body")
    expect(document.documentElement.className.trim()).toBe("existing-html")
  })

  it("updates classes when dependencies change", () => {
    const { rerender, unmount } = renderHook(({ bodyClass, htmlClass }) => useBodyClass(bodyClass, htmlClass), {
      initialProps: { bodyClass: "body-1", htmlClass: "html-1" },
    })

    expect(document.body.className.trim()).toBe("body-1")
    expect(document.documentElement.className.trim()).toBe("html-1")

    rerender({ bodyClass: "body-2", htmlClass: "html-2" })

    expect(document.body.className.trim()).toBe("body-2")
    expect(document.documentElement.className.trim()).toBe("html-2")

    unmount()

    expect(document.body.className.trim()).toBe("")
    expect(document.documentElement.className.trim()).toBe("")
  })
})