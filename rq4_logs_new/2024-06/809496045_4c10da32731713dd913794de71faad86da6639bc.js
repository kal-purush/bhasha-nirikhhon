/**
 * Create a heading element for an article
 * @param {String} title        Heading text
 * @param {String} url          URL of article
 * @param {Boolean} hasLink     If this heading should also contain a link to
 *                              the article
 * @param {String} className    A CSS class name for the heading
 * @param {String} hLevel       The heading level, default to H2
 * @returns                     Heading element
 */
export function createArticleHeading(
    title,
    url,
    hasLink,
    className,
    hLevel = "h2"
) {
    const heading = document.createElement(hLevel);
    const headingText = document.createTextNode(title);

    if (className.length > 0) {
        heading.classList.add(className);
    }

    if (hasLink) {
        const link = document.createElement("a");
        link.setAttribute("href", url);
        link.append(headingText);
        heading.append(link);
    } else {
        heading.append(headingText);
    }

    return heading;
}

/**
 * Creates a paragraph element of the article description.
 * @param {String} text Description text
 * @returns             Paragraph element with description text
 */
export function createDescription(text) {
    const paragraph = document.createElement("p");
    const paragraphText = document.createTextNode(text);
    paragraph.append(paragraphText);
    return paragraph;
}

/**
 *
 * @param {String} headingLevel Element heading level (default h2)
 * @param {String} headingText Text of heading element
 * @param {String} paragraphText Text of paragraph
 * @returns
 */
export function getErrorElement(
    headingLevel = "h2",
    headingText,
    paragraphText
) {
    const container = document.createElement("div");
    container.classList.add("error-element");

    if (headingLevel.length > 0) {
        const heading = document.createElement(headingLevel);
        const headingTextNode = document.createTextNode(headingText);
        heading.append(headingTextNode);
        heading.classList.add("error-element__heading");

        container.append(heading);
    }

    const paragraph = document.createElement("p");
    const paragraphTextNode = document.createTextNode(paragraphText);
    paragraph.append(paragraphTextNode);
    paragraph.classList.add("error-element__paragraph");

    container.append(paragraph);

    return container;
}