class Button implements DisplayNode {
    private renderedState: boolean;
    private state: boolean;
    private element: HTMLElement;
    private onColor: string;
    private offColor: string;

    public isQueuedForRender: boolean;
    public nextQueued: DisplayNode;

    constructor(elementId: string, onColor: string, offColor: string) {
        this.element = $.id(elementId);
        this.onColor = onColor;
        this.offColor = offColor;
    }

    public setState(state: boolean): void {
        Assert.bool(state);
        if (state === this.state) {
            // Same state
            return;
        }
        this.state = state;
        Display.queueForRender(this);
    }

    public render(): void {
        if (this.state === this.renderedState) {
            // State haven't changed since the last render,
            // no need to re-render anything.
            return;
        }

        this.element.style.backgroundColor = this.state ? this.onColor : this.offColor;
    }
}