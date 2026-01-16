const catalogCtl = {}

const Catalog = require('../models/catalog');
const cloudinary = require('cloudinary').v2;
const fs = require('fs-extra');

catalogCtl.getAllCatalog = async (_req, res) => {
    const catalog = await Catalog.find();
    if (catalog.length == 0) {
        res.json({ success: false, message: 'No hay lista de catálogos disponibles' });
    } else {
        res.json({ listaCatalogos: catalog, success: true });
    }

}

catalogCtl.createCatalog = async (req, res, next) => {
    const { productName, size, color, description, cost, quantity } = req.body;
    try {
        // const result = await cloudinary.uploader.upload((req.file != undefined) ? req.file.path : req.body.image, { folder: "catalog" });
        let catalog = new Catalog({
            productName,
            size,
            color,
            description,
            cost,
            quantity,
            images:[]
            // image: result.secure_url,
            // image_id: result.public_id
        });
        await catalog.save();
        // await fs.unlink(req.file.path);
        res.send({ message: 'Catalogo creado', success: true });
    } catch (err) {
        res.json({ success: false, message: 'Hubo un error en el registro, intentelo mas tarde..!' });
        next(err);
    }
}

catalogCtl.getCatalogById = async (req, res) => {
    const catalogInfo = await Catalog.findById(req.query.id);
    if (catalogInfo) {
        res.send({ success: true, productInfo: catalogInfo });
    } else {
        res.send({ success: false, productInfo: [], mesaje: 'El producto que estas leyendo no esta en la lista' });
    }

}
catalogCtl.editCatalog = async (req, res) => {
    try {
        //const result = await cloudinary.uploader.upload((req.file != undefined) ? req.file.path : req.body.image, { folder: "catalog" });
        // const updatedResult = await Catalog.findByIdAndUpdate(req.body._id, {
        //     productName,
        //     size,
        //     color,
        //     description,
        //     cost,
        //     quantity
        //     // image: result.secure_url,
        //     // image_id: result.public_id
        // });
        // await cloudinary.uploader.destroy(updateCatalog.image_id);
        // await fs.unlink(req.file.path);
        //console.log(updatedResult)
        await Catalog.findByIdAndUpdate(req.body._id, req.body);
        res.send({ message: 'Catalogo actualizado', success: true });
    } catch (err) {
        res.json({ success: false, message: 'Hubo un error en el registro, intentelo mas tarde..!' });
        next(err);
    }
}

catalogCtl.deleteCatalog = async (req, res) => {
    const photo = await Catalog.findByIdAndDelete(req.query.id);
    await cloudinary.uploader.destroy(photo.image_id);
    res.send({ success: true, message: 'Catalogo eliminado' });
}

catalogCtl.addCatalogImages = async (req, res) => {
    try {
        const result = await cloudinary.uploader.upload((req.file != undefined) ? req.file.path : req.body.image, { folder: "catalog" });

        const dataImage = {
            imageURL: result.secure_url,
            imageID: result.public_id
        }
        
        await Catalog.findOneAndUpdate({ _id: req.body._id }, { $push: { images: dataImage } }).then(function (data) {
            res.send({ message: 'Imagenes agregadas exitosamente', success: true });
        });
    } catch (err) {
        res.json({ success: false, message: 'Hubo un error en el registro, intentelo mas tarde..!' });
        next(err);
    }
}

catalogCtl.deleteImageCatalog = async (req, res) => {
    console.log(req.body);
    try {
        await Catalog.findOneAndUpdate(
            { _id: req.body._id },
            { $pull: { images: { _id: req.body.idImage } } },
            { safe: true, multi: false }
        );

        await cloudinary.uploader.destroy(req.body.imageCloudID);
        res.send({ message: 'Imagen eliminada exitosamente', success: true });

    } catch (err) {
        res.json({ success: false, message: 'Hubo un error en la eliminacion, intentelo mas tarde..!' });
    }
}

module.exports = catalogCtl;