const nodemailer = require('nodemailer');

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ message: 'Método no permitido' });
  }

  const { nombre, email, mensaje } = req.body;

  if (!nombre || !email || !mensaje) {
    return res.status(400).json({ message: 'Faltan campos requeridos' });
  }

  // Transporter con una cuenta Gmail de prueba
  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: 'TUCORREO@gmail.com',      // Cambia esto por tu correo Gmail
      pass: 'TU_CONTRASEÑA_APP'        // Usá una contraseña de aplicación
    }
  });

  try {
    await transporter.sendMail({
      from: `"Contacto desde web" <${email}>`,
      to:[ 'ariel.barrozo@gmail.com','barrozo.ariel@inta.gob.ar'], // Receptor (puede ser el mismo)
      subject: 'Mensaje del formulario de contacto en la web por Ariel B.',
      text: `Nombre: ${nombre}\nEmail: ${email}\nMensaje: ${mensaje}`
    });

    res.status(200).json({ message: 'Correo enviado correctamente' });
  } catch (error) {
    console.error('Error al enviar correo:', error);
    res.status(500).json({ message: 'Error al enviar el correo' });
  }
};