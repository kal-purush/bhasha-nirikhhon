/**
 * @jest-environment jsdom
 */

import inContentScriptOverlay, { PLASMO_OVERLAY_TAG } from '../../../../src/lib/ui/uploadWidget/inContentScriptOverlay'

describe('inContentScriptOverlay', () => {
  it(`
      if element has Plasmo overlay ID,
      returns true
    `,
  () => {
    const element = document.createElement(PLASMO_OVERLAY_TAG)

    expect(inContentScriptOverlay(element)).toBeTruthy()
  })

  it(`
      if element is contained within an element containing the Plasmo overlay ID,
      returns true
    `,
  () => {
    const element = document.createElement(PLASMO_OVERLAY_TAG)

    const childElement = document.createElement('div')
    element.appendChild(childElement)

    expect(inContentScriptOverlay(element)).toBeTruthy()
  })

  it(`
      if element is not contained within an element containing the Plasmo overlay ID,
      returns false
    `,
  () => {
    const element = document.createElement('div')

    expect(inContentScriptOverlay(element)).toBeFalsy()
  })
})