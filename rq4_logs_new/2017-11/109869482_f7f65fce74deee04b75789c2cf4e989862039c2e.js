export default function selectElement(client, selector) {
  if (typeof selector === 'object') return selector

  return client.element(selector)
}