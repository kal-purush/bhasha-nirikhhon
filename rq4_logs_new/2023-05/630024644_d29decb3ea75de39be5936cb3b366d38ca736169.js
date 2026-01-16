export const fechaLocal = (fechaSolicitada) => {
    let fecha = new Date(fechaSolicitada);
    let dia = fecha.getDate();
    let mes = fecha.getMonth();
    let anio = fecha.getFullYear();
    let meses = [
      "enero",
      "febrero",
      "marzo",
      "abril",
      "mayo",
      "junio",
      "julio",
      "agosto",
      "septiembre",
      "octubre",
      "noviembre",
      "diciembre",
    ];
    let nombreMes = meses[mes];
    let hora = fecha.getHours();
    let minutos = fecha.getMinutes();
    let periodo = hora >= 12 ? "pm" : "am";
    let hora12 = hora % 12 || 12; // Convertir a formato de 12 horas
  
    let cadenaFecha = `${dia} de ${nombreMes} del ${anio} a las ${hora12}:${minutos.toString().padStart(2, "0")} ${periodo}`;
    return cadenaFecha;
  };