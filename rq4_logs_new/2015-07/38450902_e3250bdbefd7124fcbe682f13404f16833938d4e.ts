interface tooltip_options {

    // 'top' or 'bottom' (default)
    position?: string;
    // 'title': Get tooltip content from title attribute
    // 'anchor': Use it on <a href="#id"></a>. The href attribute will be used as a selector. The matched elements will be appended to the tooltip and made visible.
    anchor?: string;
    source?: string;
    cssClass?: string;
    closeSelector?: string;
    distance?: number;

    //If false (default), close any other visible tooltip on display
    allowMultiple?: boolean;

    //Can be an HTML string, an element or a JQuery object
    //Can also be a function returning the same value type (this refer to the target element).
    //If set, source is ignored.
    content?: any;

    closeOnClickOuside?: boolean;
}

interface JQuery {
    tooltip(options?: tooltip_options, showOn?:string, selector?:string): JQuery;
    showTooltip(options?: tooltip_options): Tooltip.Tooltip;
    closeTooltip(): JQuery;
}