import Axios from "axios";
import { MediaItemType } from "./types";

const fetchLongDescriptions = (path: string) => Axios.get(path).then(
  ({ data }) => data
);

const buildSrc = (media: MediaItemType): string => {
  const { path, id, index, ext } = media;
  return `${path}/${id}-${index}.${ext}`
}

export {
  buildSrc,
  fetchLongDescriptions,
}