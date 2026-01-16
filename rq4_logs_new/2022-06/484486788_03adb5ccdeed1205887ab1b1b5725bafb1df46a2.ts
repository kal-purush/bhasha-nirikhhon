import {
  StatsUpdatedEvent,
  Subjects,
  Listener,
  NotFoundError,
} from "@type-reader/common";
import { Message } from "node-nats-streaming";
import { Bookmark } from "../../model/bookmark";

import { queueGroupName } from "./queue-group-name";

export class StatsUpdatedListener extends Listener<StatsUpdatedEvent> {
  readonly subject = Subjects.StatsUpdated;
  queueGroupName = queueGroupName;

  async onMessage(data: StatsUpdatedEvent["data"], msg: Message) {
    const { bookId, userId, pageIndex, cursorIndex, readInSec, prevText } =
      data;

    const bookmark = await Bookmark.findOne({ bookId, userId });

    bookmark!.pageIndex = pageIndex;
    bookmark!.cursorIndex = cursorIndex;
    bookmark!.totalSecOnBook += readInSec;
    if (typeof prevText !== "undefined") {
      bookmark!.prevText = prevText;
    }

    await bookmark!.save();

    msg.ack();
  }
}