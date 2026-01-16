//
// Copyright (C) Microsoft. All rights reserved.
//

/// <reference path="ContentControl.ts" />
/// <reference path="../assert.ts" />
/// <reference path="../KeyCodes.ts" />
/// <reference path="../Framework/binding/CommonConverters.ts" />

/// <disable code="SA1201" rule="ElementsMustAppearInTheCorrectOrder" justification="egregious TSSC rule"/>

module Common.Controls {
    "use strict";

    /**
     * A Button class which is templatable and provides basic button functionality
     */
    export class Button extends ContentControl {
        /** CSS class to apply to the button's root element when it's pressed */
        private static CLASS_PRESSED = "pressed";

        /** The mouse handler for the root element */
        private _mouseHandler: (e: MouseEvent) => void;

        /** The key handler function for the root element */
        private _keyHandler: (e: KeyboardEvent) => void;

        /** The root html element to attach events to and assign roles to */
        public root: HTMLElement;

        public static IsPressedPropertyName = "isPressed";

        /**
         * [ObservableProperty] Gets or sets a value indicating whether the button is currently pressed.
         */
        public isPressed: boolean;

        /**
         * The event which is fired when the button is clicked.
         * NOTE: A click can occur via keyboard, or mouse interaction
         */
        public click: EventSource<Event>;

        /**
         * Constructor
         * @param templateId The id of the template to apply to the control
         */
        constructor(templateId?: string) {
            this._mouseHandler = (e: MouseEvent) => this.onMouseEvent(e);
            this._keyHandler = (e: KeyboardEvent) => this.onKeyboardEvent(e);

            this.click = new EventSource<Event>();

            super(templateId || "Common.defaultButtonTemplate");
        }

        /**
         * Static constructor used to initialize observable properties
         */
        public static initialize(): void {
            Common.ObservableHelpers.defineProperty(Button, Button.IsPressedPropertyName, false, (obj: Button, oldValue: boolean, newValue: boolean) => obj.onIsPressedChanged(oldValue, newValue));
        }

        /**
         * Updates the control when the template has changed
         */
        public onApplyTemplate(): void {
            super.onApplyTemplate();

            if (this.rootElement) {
                if (!this.rootElement.hasAttribute("role")) {
                    // Consumers of this control are free to override this
                    // ie. A "link" is technically a button, but would override
                    // this attribute for accessibility reasons.
                    this.rootElement.setAttribute("role", "button");
                }

                this.rootElement.addEventListener("click", this._mouseHandler);
                this.rootElement.addEventListener("mousedown", this._mouseHandler);
                this.rootElement.addEventListener("mouseup", this._mouseHandler);
                this.rootElement.addEventListener("mouseleave", this._mouseHandler);
                this.rootElement.addEventListener("keydown", this._keyHandler);
                this.rootElement.addEventListener("keyup", this._keyHandler);

                // Ensure the control is in the correct state
                this.onIsPressedChanged(null, this.isPressed);
            }
        }

        /**
         * Updates the control when the template is about to change. Removes event handlers from previous root element.
         */
        public onTemplateChanging(): void {
            super.onTemplateChanging();

            if (this.rootElement) {
                this.rootElement.removeEventListener("click", this._mouseHandler);
                this.rootElement.removeEventListener("mousedown", this._mouseHandler);
                this.rootElement.removeEventListener("mouseup", this._mouseHandler);
                this.rootElement.removeEventListener("mouseleave", this._mouseHandler);
                this.rootElement.removeEventListener("keydown", this._keyHandler);
                this.rootElement.removeEventListener("keyup", this._keyHandler);
            }
        }

        /**
         * Protected override. Handles a change to the tooltip property
         */
        public onTooltipChangedOverride(): void {
            super.onTooltipChangedOverride();

            if (this.tooltip) {
                this.rootElement.setAttribute("data-plugin-vs-tooltip", this.tooltip);
                this.rootElement.setAttribute("aria-label", Common.CommonConverters.JsonHtmlTooltipToInnerTextConverter.convertTo(this.tooltip));
            } else {
                this.rootElement.removeAttribute("data-plugin-vs-tooltip");
                this.rootElement.removeAttribute("aria-label");
            }
        }

        /**
         * Dispatches a click event only if the button is enabled
         * @param e An optional event object.
         */
        public press(e?: Event): void {
            if (this.isEnabled) {
                this.click.invoke(e);
            }
        }

        /**
         * Handles a change to the isPressed property
         * @param oldValue The old value for the property
         * @param newValue The new value for the property
         */
        private onIsPressedChanged(oldValue: boolean, newValue: boolean): void {
            if (this.rootElement) {
                if (newValue) {
                    this.rootElement.classList.add(Button.CLASS_PRESSED);
                } else {
                    this.rootElement.classList.remove(Button.CLASS_PRESSED);
                }
            }
        }

        /**
         * Handles mouse events to allow the button to be interacted with via the mouse
         * @param e The mouse event
         */
        private onMouseEvent(e: MouseEvent): void {
            if (this.isEnabled) {
                var stopPropagation = false;
                switch (e.type) {
                    case "click":
                        this.rootElement.focus();
                        this.click.invoke(e);
                        stopPropagation = true;
                        break;
                    case "mousedown":
                        this.isPressed = true;
                        break;
                    case "mouseup":
                    case "mouseleave":
                        this.isPressed = false;
                        break;
                    default:
                        F12.Tools.Utility.Assert.fail("Unexpected");
                }

                if (stopPropagation) {
                    e.stopImmediatePropagation();
                    e.preventDefault();
                }
            }
        }

        /**
         * Handles keyboard events to allow the button to be interacted with via the keyboard
         * @param e The keyboard event
         */
        private onKeyboardEvent(e: KeyboardEvent): void {
            if (this.isEnabled && (e.keyCode === Common.KeyCodes.Enter || e.keyCode === Common.KeyCodes.Space)) {
                switch (e.type) {
                    case "keydown":
                        this.isPressed = true;
                        break;
                    case "keyup":
                        // Narrator bypasses normal keydown/up events and clicks
                        // directly.  Make sure we only perform a click here when
                        // the button has really been pressed.  (ie. via regular
                        // keyboard interaction)
                        if (this.isPressed) {
                            this.isPressed = false;
                            this.click.invoke(e);
                        }

                        break;
                    default:
                        F12.Tools.Utility.Assert.fail("Unexpected");
                }
            }
        }
    }

    Button.initialize();
}