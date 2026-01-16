import {
    SakTextFieldBase,
    placeholderTextProperty, placeholderTextColourProperty, isPlaceholderAnimatedProperty,
    maxlengthProperty,
    ellipsizeProperty,
    regexProperty,
    letterColourProperty,
} from "./sak-textfield.common";

export class SakTextField extends SakTextFieldBase {

    // added for TypeScript intellisense.
    nativeView: android.widget.EditText;

    // textField: android.widget.EditText;
    initialText: string;

    /**
     * Creates new native text field.
     */
    public createNativeView(): Object {
        // Create new instance of android.widget.EditText.
        let textField = new android.widget.EditText(this._context);

        textField.setMaxLines(1);

        return textField;
    }

    /**
     * Initializes properties/listeners of the native view.
     */
    initNativeView(): void {
        // Attach the owner to nativeView.
        // When nativeView is tapped we get the owning JS object through this field.
        (<any>this.nativeView).owner = this;
        super.initNativeView();
    }

    /**
     * Clean up references to the native view and resets nativeView to its original state.
     * If you have changed nativeView in some other way except through setNative callbacks
     * you have a chance here to revert it back to its original state
     * so that it could be reused later.
     */
    disposeNativeView(): void {
        // Remove reference from native view to this instance.
        (<any>this.nativeView).owner = null;

        // If you want to recycle nativeView and have modified the nativeView
        // without using Property or CssProperty (e.g. outside our property system - 'setNative' callbacks)
        // you have to reset it to its initial state here.
        super.disposeNativeView();
    }

    // transfer JS placeholderText value to nativeView.
    [placeholderTextProperty.setNative](value: string) {
        this.nativeView.setHint(value);
    }

    [placeholderTextColourProperty.setNative](value: string) {
        if (value) {
            this.nativeView.setHintTextColor(android.graphics.Color.parseColor(`#${value}`));
        }
    }

    // transfer JS maxlength value to nativeView.
    [maxlengthProperty.setNative](value: string) {
        if (value) {
            let fArray = [];
            fArray[0] = new android.text.InputFilter.LengthFilter(Number(value));
            this.nativeView.setFilters(fArray);
        }
    }

    // transfer JS ellipsize value to nativeView.
    [ellipsizeProperty.setNative](value: boolean) {
        if (value) {
            this.nativeView.setEllipsize(android.text.TextUtils.TruncateAt.END);
        }
    }

    [letterColourProperty.setNative](value: string) {
        // console.log('chegou a cor ' + value + ' : ' + `#${value}`);
        if (value) {
            this.nativeView.setTextColor(android.graphics.Color.parseColor(`#${value}`));
        }
    }

    [isPlaceholderAnimatedProperty.setNative](value: boolean) {

    }

    [regexProperty.setNative](regex: string) {

        this.nativeView.addTextChangedListener(new android.text.TextWatcher({

            beforeTextChanged(s: string, index: number, toBeReplaced: number, addedCount: number): void {

            },
            onTextChanged(s: string, index: number, replacedCount: number, addedCount: number): void {

            },
            afterTextChanged(s: android.text.Editable): void {
                if (regex && regex != "") {
                    let text = s.toString();
                    let length = s.length();
    
                    if (length > 0 && !java.util.regex.Pattern.matches(regex, text)) {
                        s.delete(length - 1, length);
                    }
                }
            }
        }));
    }
}