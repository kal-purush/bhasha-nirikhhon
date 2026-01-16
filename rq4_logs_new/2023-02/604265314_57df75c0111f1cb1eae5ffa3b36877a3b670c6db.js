
/**
 * 
 * @param {HTMLHtmlElement} element 
 */
export const clearHTML = (elementId) =>{
    const element = document.querySelector(elementId);

    if (!element)
        throw new Error(`${elementId} is not found`);

    while(element.firstChild){
        element.removeChild(element.firstChild);
    }
}