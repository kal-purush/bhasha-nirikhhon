/**
 * @module actions
 */

// Vendor modules
import { ContentRef } from "@nteract/types";

export const PUBLISH_TO_BOOKSTORE = "CORE/PUBLISH_TO_BOOKSTORE";
export interface PublishToBookstore {
  type: "CORE/PUBLISH_TO_BOOKSTORE";
  payload: {
    contentRef: ContentRef;
  };
}

export const PUBLISH_TO_BOOKSTORE_SUCCEEDED =
  "CORE/PUBLISH_TO_BOOKSTORE_SUCCEEDED";
export interface PublishToBookstoreSucceeded {
  type: "CORE/PUBLISH_TO_BOOKSTORE_SUCCEEDED";
  payload: {
    contentRef: ContentRef;
  };
}

export const PUBLISH_TO_BOOKSTORE_FAILED = "CORE/PUBLISH_TO_BOOKSTORE_FAILED";
export interface PublishToBookstoreFailed {
  type: "CORE/PUBLISH_TO_BOOKSTORE_FAILED";
  payload: {
    contentRef: ContentRef;
    error: Error;
  };
}