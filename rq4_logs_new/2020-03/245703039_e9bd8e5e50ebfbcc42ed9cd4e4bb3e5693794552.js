function Link(document) {
  this.id = document.id;
  this.url = document.get('url');
  this.clicks = document.get('clicks');
}

module.exports = Link;