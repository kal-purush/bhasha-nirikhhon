import { netCode } from "./net";
import { ContentObj } from "../model/content";

export class ContentNet extends netCode {
  deleteContent(content: ContentObj): Promise<ContentObj> {
    return window
      .fetch(`${this.getURL()}/content/${content.id}`, {
        method: "DELETE",
        redirect: "follow",
        referrerPolicy: "no-referrer",
        headers: {
          Host: window.location.origin,
          Origin: window.location.origin,
        },
      })
      .then((r) => {
        if (!r.ok) {
          throw r;
        }

        return r.json();
      });
  }
}