import express = require("express");
import dl = require("./DataLayer");
import dc = require("./DataContext");
import h = require("./helpers");
import q = require("q");

export class PublishContext {
    private _router: express.Router;

    constructor() {
        this._router = express.Router();
    }

    addDataModel(name: string, dataModel: dc.DataModel): void {
        this.addGet(name, dataModel);
        this.addPost(name, dataModel);
        this.addPatch(name, dataModel);
        this.addPut(name, dataModel);
    }
    getRouter(): express.Router {
        return this._router;
    }

    private addGet(name: string, dataModel: dc.DataModel): void {
        this._router.get("/" + name, (req, res): void => {
            var selectOptions = req.body;

            if (req.query.options) {
                selectOptions = JSON.parse(req.query.options);
            }

            dataModel.select(selectOptions)
                .then((r): void => {
                    res.json(r);
                })
                .done();
        });
        this._router.get("/" + name + "/:id", (req, res): void => {
            dataModel.selectById(req.params.id)
                .then((r): void => {
                    res.json(r);
                })
                .done();
        });
    }
    private addPost(name: string, dataModel: dc.DataModel): void {
        this._router.post("/" + name, (req, res): void => {
            dataModel.updateOrInsertAndSelect(req.body)
                .then((r): void => {
                    res.json(r);
                })
                .done();
        });
    }
    private addPatch(name: string, dataModel: dc.DataModel): void {
        this._router.patch("/" + name, (req, res): void => {
            dataModel.updateAndSelect(req.body)
                .then((r): void => {
                    res.json(r);
                })
                .done();
        });
    }
    private addPut(name: string, dataModel: dc.DataModel): void {
        this._router.put("/" + name, (req, res): void => {
            dataModel.insertAndSelect(req.body)
                .then((r): void => {
                    res.json(r);
                })
                .done();
        });
    }
}