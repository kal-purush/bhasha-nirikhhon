class Content {
  /**
   * @param {number} status
   * @param {string} content
   */
  constructor(status, content = "") {
    this.status = status;
    this.content = content;
  }
}

module.exports = Content;