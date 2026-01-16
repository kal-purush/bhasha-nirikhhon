class ScrollSpy {
    mode: string;
    buffer: number;
    container: HTMLElement | Window;
    onEnter: (element: HTMLElement, position: { top: number; left: number }) => void;
    onLeave: (element: HTMLElement, position: { top: number; left: number }) => void;
    onTick: (
        element: HTMLElement,
        position: { top: number; left: number },
        inside: boolean,
        enters: number,
        leaves: number
    ) => void;

    protected events: Array<{
        element: HTMLElement;
        handler: (event: Event) => void;
    }>;

    constructor(options: {
        mode?: string;
        buffer?: number;
        container?: HTMLElement | Window;
        onEnter?: (element: HTMLElement, position: { top: number; left: number }) => void;
        onLeave?: (element: HTMLElement, position: { top: number; left: number }) => void;
        onTick?: (element: HTMLElement, position: { top: number; left: number }) => void
    }) {
        this.mode = options.mode ? options.mode : 'vertical';
        this.buffer = options.buffer ? options.buffer : 0;
        this.container = options.container ? options.container : window;
        this.onEnter = options.onEnter ? options.onEnter : null;
        this.onLeave = options.onLeave ? options.onLeave : null;
        this.onTick = options.onTick ? options.onTick : null;

        this.events = [];
    }

    spy(element: HTMLElement, bounds: {
        min: number;
        max: number
    }): void {
        var enters: number = 0, leaves: number = 0;
        var inside: boolean = false;

        this.events.push({
            element: element,
            handler: (event: Event) => {
                var position = {
                    top: this.container instanceof Window ?
                        (<Window> this.container).pageYOffset
                        : (<HTMLElement> this.container).scrollTop,

                    left: this.container instanceof Window ?
                        (<Window> this.container).pageXOffset
                        : (<HTMLElement> this.container).scrollTop
                }

                var xy = this.mode === 'vertical' ?
                    position.top + this.buffer
                    : position.left + this.buffer;

                var min: number = bounds.min;
                var max: number;
                {
                    if (bounds.max === 0) {
                        if (this.container instanceof Window) {
                            max = (this.mode === 'vertical') ?
                                (<Window> this.container).innerWidth
                                : (<Window> this.container).outerWidth + element.offsetWidth;
                        } else {
                            max = (this.mode === 'vertical') ?
                                (<HTMLElement> this.container).clientHeight
                                : (<HTMLElement> this.container).offsetWidth + element.offsetWidth;
                        }
                    } else {
                        max = bounds.max;
                    }
                }

                if (xy >= min && xy <= max) {
                    if(!inside) {
                        inside = true;
                        enters++;
                        
                        var scrollEnter = document.createEvent('Event');
                        scrollEnter.initEvent('scrollEnter', false, true);
                        scrollEnter.position = position;

                        element.dispatchEvent(scrollEnter);

                        if (this.onEnter)
                            this.onEnter(element, position);
                    }

                    var scrollTick = document.createEvent('Event');
                    scrollTick.initEvent('scrollTick', false, true);
                    scrollTick.position = position;
                    scrollTick.tickData = { inside: inside, enters: enters, leaves: leaves };

                    element.dispatchEvent(scrollTick);

                    if (this.onTick)
                        this.onTick(element, position, inside, enters, leaves);
                } else {
                    inside = false;
                    leaves++;

                    var scrollLeave = document.createEvent('Event');
                    scrollLeave.initEvent('scrollLeave', false, true);
                    scrollLeave.position = position;

                    element.dispatchEvent(scrollLeave);

                    if (this.onLeave)
                        this.onLeave(element, position);
                }
            }
        })

        this.container.addEventListener('scroll', this.events[this.events.length - 1].handler);
    }

    unspy(element: HTMLElement, isSilent: boolean): boolean {
        var hasElement = false;

        this.events.forEach((event) => {
            if (event.element === element) {
                this.container.removeEventListener('scroll', event.handler);
                hasElement = true;

                var index = this.events.indexOf(event);
                if (index > 0) {
                    this.events.splice(index, 1);
                }
            }
        });

        if (!hasElement) {
            if (!isSilent)
                throw new Error('ScrollSpy are not spying on the element ' + element);
            else
                return false;
        } else {
            return true;
        }
    }

    unspyAll(): void {
        this.events.forEach((event) => {
            this.container.removeEventListener('scroll', event.handler);
            
            var index = this.events.indexOf(event);
            if (index > 0) {
                this.events.splice(index, 1);
            }
        });
    }
}