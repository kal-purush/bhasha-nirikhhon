using System;
using System.Collections.Generic;

namespace ISTPET_PortalEstudiantil.Models.sigafi_es;

public partial class parametros
{
    public string? codigo_institucion { get; set; }

    public string? nombreInstitucion { get; set; }

    public string? cadenaConexion { get; set; }

    public string? nombreRector { get; set; }

    public string? archivoFirma { get; set; }

    public string? archivoSello { get; set; }

    public string? emailSolicitudes { get; set; }

    public string? claveEmailSolicitudes { get; set; }

    public sbyte? activo { get; set; }

    public sbyte? permiteActualizacionCompleta { get; set; }
}