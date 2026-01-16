UIInitList.push(function() {initUIElms('gallery', Gallery)})
UIResizeList.push(function() {UIElms.gallery.map(e => e.update())})

class Gallery {
	constructor(elm) {
		this.elm = elm
    this.buttonWidth = parseFloat(this.elm.dataset.buttonWidth) || 0.05
    this.theme = this.elm.dataset.theme || 'light'
    this.items = []; for (let c of this.elm.children) this.items.push(c)
    this.index = 0
    this.items.map(e => e.style = 'position: absolute')
    this.elm.style.position = 'relative'

    this.left = createElement('div', this.elm)
    this.right = createElement('div', this.elm)
    this.left.style = 'position: absolute; left: 0; width: '+this.buttonWidth*100+'%; height: 100%;'
    this.right.style = 'position: absolute; right: 0; width: '+this.buttonWidth*100+'%; height: 100%'
    this.buttons.map(e => e.style.transition = 'font-size 50ms, text-shadow 50ms')
    this.iconLeft = createIcon("fas fa-chevron-left", this.left)
    this.iconRight = createIcon("fas fa-chevron-right", this.right)
    this.buttons.map(elm => elm.children[0].style = 'position: absolute; left: 50%; top: 50%; transform: translate(-50%, -50%);')

    this.div = createElement('div', this.elm)
    this.div.style = 'position: absolute; height: 100%; overflow: hidden'
    this.div.style.left = this.buttonWidth*100 + '%'
    this.div.style.width = (1 - this.buttonWidth*2) * 100 +'%'
    this.items.map(elm => this.div.appendChild(elm))

    this.left.addEventListener('mouseenter', function(e){
      if (this.index > 0) {
        this.left.style.fontSize = this.buttonWidth*this.elm.clientWidth+'px'
        this.left.style.textShadow = '0 0 0.1em gray'
      }
    }.bind(this))
    this.right.addEventListener('mouseenter', function(e){
      if (this.index < this.items.length-1) {
        this.right.style.fontSize = this.buttonWidth*this.elm.clientWidth+'px'
        if (this.theme === 'light') this.right.style.textShadow = '0 0 0.1em gray'
        if (this.theme === 'dark') this.right.style.textShadow = '0 0 0.1em black'
      }
    }.bind(this))
    this.buttons.map(elm => elm.addEventListener('mouseleave', function(e){
      elm.style.fontSize = this.buttonWidth*0.8*this.elm.clientWidth+'px'
      elm.style.textShadow = 'none'
    }.bind(this)))
    this.left.addEventListener('mousedown', function(e){this.index = Math.max(this.index-1, 0); this.update()}.bind(this))
    this.right.addEventListener('mousedown', function(e){this.index = Math.min(this.index+1, this.items.length-1); this.update()}.bind(this))

    this.update()
    this.buttons.map(elm => elm.style.fontSize = this.buttonWidth*0.8*this.elm.clientWidth+'px')
	}
  get width() {
    if (this.elm.dataset.width.includes('%')) return parseFloat(this.elm.dataset.width)/100 * this.elm.parentElement.clientWidth
    else if (this.elm.dataset.width.includes('h') && !this.elm.dataset.height.includes('w')) return parseFloat(this.elm.dataset.width) * this.height
    else return parseFloat(this.elm.dataset.width)
  }
  get height() {
    if (this.elm.dataset.height.includes('%')) return parseFloat(this.elm.dataset.height)/100 * this.elm.parentElement.clientHeight
    else if (this.elm.dataset.height.includes('w') && !this.elm.dataset.width.includes('h')) return parseFloat(this.elm.dataset.height) * this.width
    else return parseFloat(this.elm.dataset.height)
  }
  get buttons() { return [this.left, this.right] }
  disableButton(i) {
    if (this.theme === 'light') this.buttons[i].style.color = '#aaa'
    else if (this.theme === 'dark') this.buttons[i].style.color = '#666'
    this.buttons[i].style.fontSize = this.buttonWidth*0.8*this.elm.clientWidth+'px'
    this.buttons[i].style.textShadow = 'none'
  }
  enableButton(i) {
    if (this.theme === 'light') this.buttons[i].style.color = 'black'
    else if (this.theme === 'dark') this.buttons[i].style.color = 'white'
  }
  update() {
    this.elm.style.width = this.width+'px'; this.elm.style.height = this.height+'px'
    this.items.map((elm, i) => {
      fit(elm)
      elm.style.left = 105*(i-this.index)+50+'%'
      elm.style.top = '50%';
      elm.style.transform = 'translate(-50%, -50%)'
      elm.style.transition = 'transform 0.5s, left 0.5s'
    })
    if (this.index == 0) this.disableButton(0)
    else this.enableButton(0)
    if (this.index == this.items.length-1) this.disableButton(1)
    else this.enableButton(1)
  }
}