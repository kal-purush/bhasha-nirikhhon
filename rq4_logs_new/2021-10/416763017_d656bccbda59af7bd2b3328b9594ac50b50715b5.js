class ElianCodesBg {
    static get inputProperties() { return ['--color-for-bg']; }
    static get inputArguments() { return ['<color>']; }

    paint(ctx, size, props) {
        const fillColor = props.get('--color-for-bg')
        ctx.fillStyle = fillColor;
        ctx.fillRect(size.width / 2, 0, size.width, size.height);
    }
}

registerPaint('eliancodes-bg', ElianCodesBg)