import { Eti } from '../eti/eti.model';
import multer = require('multer');
import * as mongoose from 'mongoose';
import * as _ from 'lodash';
let ObjectId = mongoose.Types.ObjectId

var storage = multer.diskStorage({
  // destino del fichero
  destination: function (req, file, cb) {
    cb(null, __dirname + '/../../../uploads')
  },
  // renombrar fichero
  filename: function (req, file, cb) {
    cb(null, file.originalname);
  }
});
var upload = multer({ storage: storage }).single('file');

export function uploadFile(req, res) {
  Eti.findById(req.params.etiId)
    .then(eti => {
      upload(req, res, function(err) {
        if(err) {
          console.log(err);
          res.status(500);
        }
        console.log(req.params);
        let inscripcionIndex = _.findIndex(eti.inscripciones, {'_id': ObjectId(req.params.inscripcionId)})
        console.log(inscripcionIndex);
        eti.inscripciones[inscripcionIndex].comprobante = req.file.filename;
        eti.save()
        .then(eti => {
          res.status(200).send();
        })
      });
  })
  .catch(err => {
    res.status(500);
  })
}