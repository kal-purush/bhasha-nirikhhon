export const contador = (className) =>{
  const contadores = document.querySelectorAll(className)
  const velocidad = 500
  debugger
  const animarContadores = () => {
    for (const contador of contadores) {
      
      const actualizar_contador = () => {
        
        let cantidad_maxima = + contador.dataset.cantidadTotal
        let valor_actual = + contador.innerText
        let incremento = cantidad_maxima / velocidad
        
        debugger
        
        if (valor_actual < cantidad_maxima) {
          
          contador.innerText = Math.ceil(valor_actual + incremento)
          setTimeout(actualizar_contador, 5)
          
        } else {
          
          contador.innerText = cantidad_maxima
        }
      }
      
      actualizar_contador()
    }
  }
  
  animarContadores()
}