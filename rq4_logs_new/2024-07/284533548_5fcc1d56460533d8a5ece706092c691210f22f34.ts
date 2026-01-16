/* Helper functions. */

function getUserPreferredColorScheme() {
  return window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark"
}

function getTheme() {
  return localStorage.getItem("theme") ?? getUserPreferredColorScheme()
}

function drawExcalidraw(el, imgSuffix, theme: "light" | "dark") {
  if (el.localName == "img") {
    let img = document.createElement("img")
    img.src = `${el.src}.${theme}.${imgSuffix}`
    el.replaceWith(img)
  }
 return el
}

function redrawExcalidraw(img, targetTheme: "light" | "dark", imageSuffix) {
  let srcParts = img.src.split(".")
  srcParts.splice(-2, 1, targetTheme)
  img.src = srcParts.join(".")

  return img
}

/* On each page navigation, draw all Excalidraw images.

All image elements with sources ending with `.excalidraw` are
replaced with ones that point to the corresponding image depending
on the light or dark mode setting.
*/
document.addEventListener("nav", (e) => {
  let theme = getTheme()
  let imgSuffix = "svg"  // TODO: Make configurable.
  
  Object.values(document.getElementsByTagName("article")[0]
                        .getElementsByTagName("img")).forEach(img => {
    if (img.src.endsWith(".excalidraw")) {
      drawExcalidraw(img, imgSuffix, theme)
    }
  })
})

/* On light or dak mode toggle, redraw all Excalidraw images.

This ensures that Excalidraw drawings always respect the chosen color scheme.
*/
const toggleSwitch = document.querySelector("#darkmode-toggle") as HTMLInputElement
toggleSwitch.addEventListener("change", (e) => {
  let targetTheme = getTheme() == "dark" ? "light" : "dark"
  let currentTheme = targetTheme == "dark" ? "light" : "dark"
  let imgSuffix = "svg"  // TODO: Make configurable.

  Object.values(document.getElementsByTagName("article")[0]
                        .getElementsByTagName("img")).forEach(img => {
    if (img.src.endsWith(`.excalidraw.${currentTheme}.${imgSuffix}`)) {
      redrawExcalidraw(img, targetTheme, imgSuffix)
    }
  })
})