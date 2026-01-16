import { WordAction } from "../store"
import update from "immutability-helper";
import { without, isUndefined } from "lodash"

export interface SavedWord {
  readonly word: string;
  readonly meaning: string;
  readonly context: string;
}

export interface SavedChunks {
  [textSourceId: string]: {
    [chunkId: number]: SavedWord[];
  };
}

export function savedChunksReducer(savedChunks: SavedChunks = {}, action: WordAction) {
  switch (action.type) {
    case "SAVE_WORD":
      const sourceId = action.textSourceId,
        chunkId = action.chunkId;
      if (isUndefined(savedChunks[sourceId])) {
        savedChunks = update(savedChunks, {
          $merge: {
            [sourceId]: { [chunkId]: [] }
          }
        });
      } else if (isUndefined(savedChunks[sourceId][chunkId]))
        savedChunks = update(savedChunks, {
          [sourceId]: {
            $merge: {
              [chunkId]: []
            }
          }
        });
      return update(savedChunks, {
        [sourceId]: {
          [chunkId]: {
            $push: [action.obj]
          }
        }
      });
    case "REMOVE_LOCAL_TEXT_SOURCE":
      return update(savedChunks, {
        $unset: [action.sourceIndex.id]
      });
    default:
      return savedChunks;
  }
}