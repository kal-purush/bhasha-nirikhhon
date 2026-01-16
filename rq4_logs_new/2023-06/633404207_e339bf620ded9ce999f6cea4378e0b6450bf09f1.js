import { throttle } from 'throttle-debounce';
import gsap from 'gsap';
import Experience from '../Experience';

export default class Cursor {
    constructor() {
        this.getElements()

        this.onCursorMoveBound = throttle(5, this.onCursorMove.bind(this))
        this.onCursorEnterBound = this.onCursorEnter.bind(this)
        this.onCursorLeaveBound = this.onCursorLeave.bind(this)

        this.events()
        this.init()

    }

    getElements() {
        this.cursor = document.querySelector('.cursor')
    }

    events() {
        window.addEventListener('mousemove', this.onCursorMoveBound)
    }

    onCursorMove(e) {
        gsap.to(this.cursor, {
            x: e.clientX - this.cursorSizes.width / 2,
            y: e.clientY - this.cursorSizes.height / 2,
            ease: 'power2.out'
        })
    }

    onCursorEnter() {
        gsap.to(this.cursor.querySelector('circle'), {
            scale: 0.7,
            duration: 0.2
        })
    }

    onCursorLeave() {
        gsap.to(this.cursor.querySelector('circle'), {
            scale: 1,
            duration: 0.2
        })
    }

    init() {
        this.cursorSizes = {
            width: this.cursor.getBoundingClientRect().width,
            height: this.cursor.getBoundingClientRect().height
        }
    }

    update() { }

    destroy() {
        window.removeEventListener('mousemove', this.onCursorMoveBound)
        this.onCursorMoveBound = null
        this.onCursorEnterBound = null
        this.onCursorLeaveBound = null
    }
}