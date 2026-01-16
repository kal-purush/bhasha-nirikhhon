function esSueloClase1(pendiente, erosión, movimientosEnMasa, drenajeNatural, inundaciones, encharcamientos, profundidadEfectiva, textura, fragmentosEnElSuelo, pedregosidadSuperficial, afloramientoRocoso, fertilidad, contenidoDeSales, contenidoDeSodio, salesYSodio, caMg, saturacionDeAluminio, distribucionDeLluvias, pisoTermico, condicionDeHumedad, temperaturaHeladas) {
    // Verificamos que la pendiente esté entre 0 y 3%
    if (pendiente < 0 || pendiente > 3) {
      return false;
    }
  
    // Verificamos que no haya erosión
    if (erosión !== "No hay") {
      return false;
    }
  
    // Verificamos que no haya movimientos en masa
    if (movimientosEnMasa > 0) {
      return false;
    }
  
    // Verificamos que el drenaje natural sea bien drenado
    if (drenajeNatural !== "Bien drenado") {
      return false;
    }
  
    // Verificamos que no haya inundaciones
    if (inundaciones.frecuencia || inundaciones.duracion) {
      return false;
    }
  
    // Verificamos que no haya encharcamientos
    if (encharcamientos.frecuencia || encharcamientos.duracion) {
      return false;
    }
  
    // Verificamos que la profundidad efectiva sea mayor a 100 cm
    if (profundidadEfectiva < 100) {
      return false;
    }
  
    // Verificamos que la textura sea francosa fina
    if (textura.familia !== "Francosa fina") {
      return false;
    }
  
    // Verificamos que el contenido de fragmentos en el suelo sea menor a 3%
    if (fragmentosEnElSuelo > 3) {
      return false;
    }
  
    // Verificamos que la pedregosidad superficial sea menor a 0.1%
    if (pedregosidadSuperficial > 0.1) {
      return false;
    }
  
    // Verificamos que el afloramiento rocoso sea menor a 0.1%
    if (afloramientoRocoso > 0.1) {
      return false;
    }
  
    // Verificamos que la fertilidad sea muy alta, alta o media
    if (fertilidad !== "Muy alta" && fertilidad !== "Alta" && fertilidad !== "Media") {
      return false;
    }
  
    // Verificamos que el contenido de sales sea menor a 0
    if (contenidoDeSales > 0) {
      return false;
    }
  
    // Verificamos que el contenido de sodio sea menor a 0
    if (contenidoDeSodio > 0) {
      return false;
    }
  
    // Verificamos que el porcentaje de Na intercambiable sea menor a 0
    if (salesYSodio.porcentaje > 0) {
      return false;
    }
  
    // Verificamos que el Ca/Mg sea normal
    if (caMg !== "Normal") {
      return false;
    }
  
    // Verificamos que la saturación de aluminio sea menor a 15%
    if (saturaciónDeAluminio > 15) {
      return false;
    }
  
    // Verificamos que la distribución de lluvias sea 1
    if (distribucionDeLluvias !== 1) {
      return false;
    }
  
    // Verificamos que la clase del piso térmico sea cálido, templado o frío
    if (pisoTermico !== "Cálido" && pisoTermico !== "Templado" && pisoTermico !== "Frío") {
      return false;
    }
  
    // Verificamos que la condición de humedad sea húmedo-seco
    if (condicionDeHumedad !== "Húmedo-seco") {
      return false;
    }
  
    // Verificamos que la temperatura sea mayor a 12°C sin heladas
    if (temperaturaHeladas < 12 || temperaturaHeladas.frecuencia > 0) {
      return false;
    }
  
    // Si todos los límites de variación se cumplen, entonces el suelo es de clase 1
    return true;
  }
  