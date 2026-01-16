import { Admin } from "../models/adminModel";





const createAdmin = async (req, res) => {
  try {
    const nuevoAdmin = new Admin({
      nombre: req.body.nombre,
      apellido: req.body.apellido,
      documento: req.body.documento,
      pic_url: req.body.pic_url
    });

    const adminCreado = await nuevoAdmin.save();
    res.status(201).json(adminCreado);
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
};

export { createAdmin };




const getAdminByNombreDocumento = async (req, res) => {
    const { nombre, documento } = req.body;
    try {
      const admin = await Admin.findOne({ nombre, documento });
      if (!admin) {
        return res.status(404).json({ message: "Admin no encontrado" });
      }
      res.json(admin);
    } catch (error) {
      res.status(500).json({ message: error.message });
    }
  };


  export{getAdminByNombreDocumento}