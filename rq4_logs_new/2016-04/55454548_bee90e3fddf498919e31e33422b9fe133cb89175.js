Ext.define('Mba.ux.field.mask.Mask', {
    extend: 'Ext.mixin.Mixin',

    regexMask: /[0-9a-zA-Z]/,

    config : {
        maxLength: 100,
        mask: null
    },

    initialize: function() {
        if (this.getMask()) {
            this.getMask().length && this.setMaxLength(this.getMask().length);
            this.on({
                scope: this,
                keyup: 'onMask'
            });
        }
    },

    /**
     * @method
     * Atribui valor mascarado para o campo
     * @param {Object} field
     * @param {Object} event
     */
    onMask: function(field, event) {
        field.setValue(this.format(this.getComponent().getValue()));
    },

    /**
     * @method
     * Retorna valor mascarado caso mascara seja definida
     *
     * @param {String} value
     * @return {String}
     */
    format: function(value) {
        return (this.getMask() && this.getMask().length)
            ? this.toPattern(value, this.getMask())
            : value;
    },

    /**
     * @method
     * Retorna valor mascarado
     *
     * @private
     * @param {String} value
     * @param {String} mask
     * @return {String}
     */
    toPattern: function(value, mask) {
        var DIGIT = "9", ALPHA = "A",
            index = 0, i,
            output = mask.split(""),
            values = value.toString().replace(/[^0-9a-zA-Z]/g, "");

        for (var i = 0; i < output.length; i++) {
            if (index >= values.length) {
                break;
            }

            if ((output[i] === DIGIT && values[index].match(/[0-9]/)) || (output[i] === ALPHA && values[index].match(/[a-zA-Z]/))) {
                output[i] = values[index++];
            } else if (output[i] === DIGIT || output[i] === ALPHA) {
                output = output.slice(0, i);
            }
        }
        return output.join("").substr(0, i);
    },

    applyValue: function(value) {
        if (!value) {
            return null;
        }

        value = value.toString();
        return this.format(value);
    },

    getValue: function() {
        var value = this.callParent(),
            valueRaw = "", char;

        for (var index = 0; index < value.length; index++) {
            char = value.substring(index, index+1);
            if (char.match(this.regexMask)) {
                valueRaw += char;
            }
        }
        return valueRaw;
    }
});