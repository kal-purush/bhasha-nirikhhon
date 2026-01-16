// Imports
const express = require('express');
const { createClient } = require('redis');
const {palabras} = require('./palabras-GrupoGramatical.js')
const app = express();
const PORT = 3001
const cors = require ('cors')
// Middlewares
app.use(cors('*'))
app.use(express.json());
//Obtener grupos gramaticales
const gruposGramaticales = []
// Filtrar palabras por grupo gramatical
const nombreTopónimo = []
const preposición = []
const verbo_Transitivo_intransitivo_pronominal_Infinitivo = []
const otro_expresión = []
const nombre_común=[]
const adverbio =[] 
const pronombre =[]
const conjunción =[] 
const nombre_compuesto =[]  
const verbo_intransitivo_infinitivo =[]  
const verbo_intransitivo_pronominal_infinitivo =[]  
const otro_locución =[]
const verbo_conjugado =[] 
const otro_onomatopeya =[]
const verbo_transitivo_pronominal_infinitivo=[]  
const otro_interjección =[]
const otro_contracción  =[]
const adjetivo  =[]
const verbo_transitivo_intransitivo_infinitivo =[] 
const artículo =[]
const otro_aumentativodiminutivo=[]  
const nombre_propio  =[]
const otro_abreviatura  =[]
const verbo_irregular_infinitivo=[] 

palabras.map(palabra=>{
  if(gruposGramaticales.includes(palabra.grupoGramatical)===false){
    gruposGramaticales.push(palabra.grupoGramatical)
   }
 if(palabra.grupoGramatical==='nombre/topónimo'&&nombreTopónimo.includes(palabra.palabra)===false){
 
nombreTopónimo.push(palabra.palabra)
 }
 if(palabra.grupoGramatical==='preposición'&&preposición.includes(palabra.palabra)===false){
 
  preposición.push(palabra.palabra)
  }
  if(palabra.grupoGramatical==='verbo/transitivo_intransitivo_pronominal/infinitivo'&&nombreTopónimo.includes(palabra.palabra)===false&& verbo_Transitivo_intransitivo_pronominal_Infinitivo .includes(palabra.palabra)===false){
 
    verbo_Transitivo_intransitivo_pronominal_Infinitivo.push(palabra.palabra)
     }
  if(palabra.grupoGramatical==='otro/expresión'&&otro_expresión.includes(palabra.palabra)===false){
 
  otro_expresión.push(palabra.palabra)
   }
 if(palabra.grupoGramatical==='nombre/común'&&nombre_común.includes(palabra.palabra)===false){
 
  nombre_común.push(palabra.palabra)
  }
  if(palabra.grupoGramatical==='adverbio'&&adverbio.includes(palabra.palabra)===false){
 
    adverbio.push(palabra.palabra)
    }
    if(palabra.grupoGramatical==='pronombre'&&pronombre.includes(palabra.palabra)===false){
 
      pronombre.push(palabra.palabra)
      }
      if(palabra.grupoGramatical==='conjunción'&&conjunción.includes(palabra.palabra)===false){
 
        conjunción.push(palabra.palabra)
        }
        if(palabra.grupoGramatical==='nombre/compuesto'&&nombre_compuesto.includes(palabra.palabra)===false){
 
          nombre_compuesto.push(palabra.palabra)
          }
          if(palabra.grupoGramatical==='verbo/intransitivo/infinitivo'&&verbo_intransitivo_infinitivo.includes(palabra.palabra)===false){
 
            verbo_intransitivo_infinitivo.push(palabra.palabra)
            }
            if(palabra.grupoGramatical==='verbo/intransitivo_pronominal/infinitivo'&&verbo_intransitivo_pronominal_infinitivo.includes(palabra.palabra)===false){
 
              verbo_intransitivo_pronominal_infinitivo.push(palabra.palabra)
              }
              if(palabra.grupoGramatical==='otro/locución'&&otro_locución.includes(palabra.palabra)===false){
 
                otro_locución.push(palabra.palabra)
                }
                if(palabra.grupoGramatical==='verbo//conjugado////'&&verbo_conjugado.includes(palabra.palabra)===false){
 
                  verbo_conjugado.push(palabra.palabra)
                  }
                  if(palabra.grupoGramatical==='otro/onomatopeya'&&otro_onomatopeya.includes(palabra.palabra)===false){
 
                  otro_onomatopeya.push(palabra.palabra)
                    }
                    if(palabra.grupoGramatical==='verbo/transitivo_pronominal/infinitivo'&&verbo_transitivo_pronominal_infinitivo.includes(palabra.palabra)===false){
 
                      verbo_transitivo_pronominal_infinitivo.push(palabra.palabra)
                      }
                      if(palabra.grupoGramatical==='otro/interjección'&&otro_interjección.includes(palabra.palabra)===false){
 
                        otro_interjección.push(palabra.palabra)
                        }
                        if(palabra.grupoGramatical==='otro/contracción'&&otro_contracción.includes(palabra.palabra)===false){
 
                          otro_contracción.push(palabra.palabra)
                          }
                          if(palabra.grupoGramatical==='adjetivo'&&adjetivo.includes(palabra.palabra)===false){
 
                            adjetivo.push(palabra.palabra)
                            }
                            if(palabra.grupoGramatical==='verbo/transitivo_intransitivo/infinitivo'&&verbo_transitivo_intransitivo_infinitivo.includes(palabra.palabra)===false){
 
                              verbo_transitivo_intransitivo_infinitivo.push(palabra.palabra)
                              }
                              if(palabra.grupoGramatical==='artículo'&&artículo.includes(palabra.palabra)===false){
 
                                artículo.push(palabra.palabra)
                                }
                                if(palabra.grupoGramatical==='otro/aumentativodiminutivo'&&otro_aumentativodiminutivo.includes(palabra.palabra)===false){
 
                                  otro_aumentativodiminutivo.push(palabra.palabra)
                                  }
                                  if(palabra.grupoGramatical==='nombre/propio'&&nombre_propio.includes(palabra.palabra)===false){
 
                                    nombre_propio.push(palabra.palabra)
                                    }
                                    if(palabra.grupoGramatical==='otro/abreviatura'&&otro_abreviatura.includes(palabra.palabra)===false){
 
                                      otro_abreviatura.push(palabra.palabra)
                                      }
                                      if(palabra.grupoGramatical==='verbo/irregular/infinitivo'&&verbo_irregular_infinitivo.includes(palabra.palabra)===false){
 
                                        verbo_irregular_infinitivo.push(palabra.palabra)
                                        }
})



app.get('/',async(req,res)=>{
  gruposGramaticales.sort(function(a,b){
    return a.localeCompare(b)
  })
  res.json({gruposGramaticales})
})

  app.get('/nombre/toponimo',async(req,res)=>{
 nombreTopónimo.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({nombreTopónimo})
  })

  app.get('/preposicion',async(req,res)=>{
 preposición.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({preposición})
  })
  app.get('/verbo/transitivo_intransitivo_pronominal/infinitivo',async(req,res)=>{
    verbo_Transitivo_intransitivo_pronominal_Infinitivo.sort(function(a,b){
      return a.localeCompare(b)
    })
    res.json({verbo_Transitivo_intransitivo_pronominal_Infinitivo})
  })
  app.get('/otro/expresion',async(req,res)=>{
 otro_expresión.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({otro_expresión})
  })
  app.get('/nombre/comun',async(req,res)=>{
 nombre_común.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({nombre_común})
  })
  app.get('/adverbio',async(req,res)=>{
 adverbio.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({adverbio})
  })
  app.get('/pronombre',async(req,res)=>{
 pronombre.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({pronombre})
  })
  app.get('/conjuncion',async(req,res)=>{
 conjunción.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({conjunción})
  })
  app.get('/nombre/compuesto',async(req,res)=>{
 nombre_compuesto.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({nombre_compuesto})
  })
  app.get('/verbo/intransitivo/infinitivo',async(req,res)=>{
    verbo_intransitivo_infinitivo.sort(function(a,b){
      return a.localeCompare(b)
    })
    res.json({verbo_intransitivo_infinitivo})
  })
  app.get('/verbo/intransitivo_pronominal/infinitivo',async(req,res)=>{
    verbo_intransitivo_pronominal_infinitivo.sort(function(a,b){
      return a.localeCompare(b)
    })
    res.json({verbo_intransitivo_pronominal_infinitivo})
  })
  app.get('/otro/locucion',async(req,res)=>{
 otro_locución.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({otro_locución})
  })
  app.get('/verbo/conjugado',async(req,res)=>{
 verbo_conjugado.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({verbo_conjugado})
  })
  app.get('/otro/onomatopeya',async(req,res)=>{
 otro_onomatopeya.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({otro_onomatopeya})
  })
  app.get('/verbo/transitivo_pronominal/infinitivo',async(req,res)=>{
 verbo_intransitivo_pronominal_infinitivo.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({verbo_transitivo_pronominal_infinitivo})
  })
  app.get('/otro/interjeccion',async(req,res)=>{
 otro_interjección.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({otro_interjección})
  })
  app.get('/otro/contraccion',async(req,res)=>{
 otro_contracción.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({otro_contracción})
  })
  app.get('/adjetivo',async(req,res)=>{
 adjetivo.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({adjetivo})
  })
  app.get('/verbo/transitivo_intransitivo/infinitivo',async(req,res)=>{
 verbo_transitivo_intransitivo_infinitivo.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({verbo_transitivo_intransitivo_infinitivo})
  })
  app.get('/articulo',async(req,res)=>{
 artículo.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({artículo})
  })
  app.get('/otro/aumentativodiminutivo',async(req,res)=>{
 otro_aumentativodiminutivo.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({otro_aumentativodiminutivo})
  })
  app.get('/nombre/propio',async(req,res)=>{
 nombre_propio.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({nombre_propio})
  })
  app.get('/otro/abreviatura',async(req,res)=>{
 otro_abreviatura.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({otro_abreviatura})
  })
  app.get('/verbo/irregular/infinitivo',async(req,res)=>{
 verbo_irregular_infinitivo.sort(function(a,b){
  return a.localeCompare(b)
})
    res.json({verbo_irregular_infinitivo})
  })

// Arrancar Servidor
app.listen(PORT, () => {
  console.log(`Server is running `);
});