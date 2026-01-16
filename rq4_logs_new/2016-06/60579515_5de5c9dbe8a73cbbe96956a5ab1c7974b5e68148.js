   function ColumnSelector(keys, keysSelected) {
        this._keys = keys;
        this._keysSelected = keysSelected;
    };

    ColumnSelector.prototype.getSelected = function() {
        alert(this._keys);
    };