import { Comprobante } from './comprobante.model';

export function createComprobante(req, res) {
  Comprobante.create(req.body)
  .then(comprobante => {
    res.status(201).json(comprobante);
  })
  .catch(err => {
    res.status(500).send();
  });
}

export function getComprobantes(req, res) {
  Comprobante.find({"eti": req.params.etiId})
  .then(comprobantes => {
    res.status(200).json(comprobantes);
  })
  .catch(err => {
    res.status(500).send();
  })
}