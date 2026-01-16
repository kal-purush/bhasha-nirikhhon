import { Base } from "./base.js";

export class Topic extends Base {
  constructor(topics) {
    super();
    this.topics = topics;
  }

  //get topics

  getTopics() {
    let list = this.topics.map((obj) => {
      return `<input type='button' class='btn-add' value='${obj}'>`;
    });

    return list.join("");
    return list;
  }

  render() {
    this.setContent("buttons", this.getTopics());
  }
}