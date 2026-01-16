import React from "react";
import { IParsedData } from "./markdownParser";

/**
 * Recursive function for create reactv elements
 * @param childrenElements elements data from markdown parser
 * @returns React Elements
 */
export const createReactChild = (
  childrenElements: IParsedData,
): React.ReactElement => {
  if (!childrenElements.children || childrenElements.children.length === 0) {
    return React.createElement(
      childrenElements.textType,
      childrenElements.attributes ?? {},
      childrenElements.content,
    );
  } else {
    return React.createElement(
      childrenElements.textType,
      childrenElements.attributes ?? {},
      childrenElements.children.map((child) => createReactChild(child)),
    );
  }
};