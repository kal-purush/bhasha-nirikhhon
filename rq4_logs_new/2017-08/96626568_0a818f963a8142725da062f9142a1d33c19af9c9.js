export default class BTree {
  constructor (a, b, type = 'horisontal', length = 50, lengthFromEnd = false) {
    this.a = a
    this.b = b
    this.type = type
    this.length = length
    this.lengthFromEnd = lengthFromEnd
  }
  slice (item, newItem, type = 'horisontal', length = 50, lengthFromEnd = false) {
    if (item === 1) {
      this.a = new BTree(this.a, newItem, type, length, lengthFromEnd)
      return this.a
    } else {
      this.b = new BTree(this.b, newItem, type, length, lengthFromEnd)
      return this.b
    }
  }
  allItems (x = 0, y = 0, w = 700, h = 400, items = []) {
    const abox = {
      left: x,
      top: y,
      width: this.type === 'horisontal' ? w : (this.lengthFromEnd ? w - this.length : this.length),
      height: this.type !== 'horisontal' ? h : (this.lengthFromEnd ? h - this.length : this.length)
    }
    const bbox = {
      left: this.type === 'horisontal' ? x : x + (this.lengthFromEnd ? w - this.length : this.length),
      top: this.type !== 'horisontal' ? y : y + (this.lengthFromEnd ? h - this.length : this.length),
      width: this.type === 'horisontal' ? w : (this.lengthFromEnd ? this.length : w - this.length),
      height: this.type !== 'horisontal' ? h : (this.lengthFromEnd ? this.length : h - this.length)
    }

    if (this.a && this.a.type) {
      this.a.allItems(abox.left, abox.top, abox.width, abox.height, items)
    } else if (this.a) {
      items.push({
        box: abox,
        component: this.a
      })
    } else {
    }

    if (this.b && this.b.type) {
      this.b.allItems(bbox.left, bbox.top, bbox.width, bbox.height, items)
    } else if (this.b) {
      items.push({
        box: bbox,
        component: this.b
      })
    } else {
    }
    return items
  }
}