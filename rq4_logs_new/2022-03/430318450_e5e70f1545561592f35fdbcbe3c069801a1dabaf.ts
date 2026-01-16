import type { PlayerOptions } from "../types";

function generateID(): string {
  return `retell-${Math.floor(Math.random() * 100000)}`;
}

export function validateOptions(userOptions: Partial<PlayerOptions>): PlayerOptions {
  userOptions = userOptions || {}
  const defaultOptions = {
    url: window.location.href,
    widget: 'default',
  } as PlayerOptions;

  const options = {
    ...defaultOptions,
    ...userOptions
  } as PlayerOptions;

  if (!isValidUrl(options.url)) {
    throw new Error('Retell: Invalid URL')
  }
  options.id = generateID();
  options.url = normalizeUrl(options.url)

  return options
}

 export function normalizeUrl(raw_url: string): string {
  const url = new URL(raw_url)
  return `${url.protocol}//${url.host}${url.pathname}`
}

export function makeQuery(options: any): string {
  const searchParams = new URLSearchParams(options)
  return `?${searchParams.toString()}`
}

export default function isValidUrl(url: string): boolean {
  if (!url.startsWith('http')) {
    return false
  }

  try {
    const { pathname } = new URL(url)
    if (pathname.includes('//')) { return false }
  } catch (_) {
    return false
  }

  return true
}

export function getLastDOMElement(selector: string): Element | undefined {
  return Array.from(document.querySelectorAll(selector)).pop()
}

export function createIFrame  (widgetURL: string) {
  return (options: PlayerOptions) => {
    const queryOptions = makeQuery(options)
    const iframe = document.createElement('iframe')
    const iframeSrc = widgetURL + queryOptions

    iframe.setAttribute('src', iframeSrc)
    iframe.className = 'retell-player'
    iframe.style.background = 'transparent'
    iframe.style.visibility = 'visible'
    iframe.style.border = '0'
    iframe.style.width = '0px'
    iframe.style.height = '0px'
    iframe.style.maxHeight = '0px'
    iframe.style.minHeight = '0px'
    iframe.dataset.retellId = options.id

    return iframe
  }
}