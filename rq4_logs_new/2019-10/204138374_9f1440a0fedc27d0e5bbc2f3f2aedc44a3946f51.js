class Journey {

    constructor(entry) {

        this._entry = entry;
        this._exit = undefined;
    }

    details() {
        return {
            entry: this._entry,
            exit: this._exit
        }
    }

    end(atStation) {
        this._exit = atStation;
    }
}

module.exports = Journey;