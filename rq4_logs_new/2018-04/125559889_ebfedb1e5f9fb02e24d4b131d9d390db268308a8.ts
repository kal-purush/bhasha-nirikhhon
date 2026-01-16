async function addLiInfo (text: string, classAndId?: {id?: string, theClass?: string}): Promise<HTMLElement> {
  const li: HTMLElement = document.createElement('li')

  // Put text into a text node and append it
  const textNode: Text = document.createTextNode(text)
  li.appendChild(textNode)

  if (classAndId) {
    // Check if id is provided and add it
    if (classAndId.id) {
      li.id = classAndId.id
    }
    // Check if class is provided and add it
    if (classAndId.theClass) {
      li.classList.add(classAndId.theClass)
    }
  }

  return li
}

export {addLiInfo}