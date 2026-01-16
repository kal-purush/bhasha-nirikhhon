class DOMNodeCollection {
  constructor(nodeList) {
    this.nodeList = nodeList;
    this.render();
  }

  render() {
    return this.nodeList;
  }

};

module.exports = DOMNodeCollection;