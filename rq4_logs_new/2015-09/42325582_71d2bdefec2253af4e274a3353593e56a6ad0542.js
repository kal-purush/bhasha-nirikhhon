function Queue() {
}

Queue.prototype.enqueue = function(item) {
  Array.prototype.unshift.apply(this, arguments);
}

Queue.prototype.dequeue = function() {
  return Array.prototype.pop.apply(this);
}

module.exports = Queue