export let FORM_PARAM_COMPROBANTES = {
  tipo_formulario: "mini",
  titulo: "Comprobante",
  btn: "Guardar",
  alertas: true,
  modelo: "Comprobante",
  campos: [
    {
      etiqueta: "input",
      claseGrid: "col-lg-6 col-md-6 col-sm-6 col-xs-6",
      nombre: "NumInicial",
      label_i18n: "num_inicial",
      placeholder_i18n: "num_inicial",
      requerido: true,
      length: "10",
      pattern: "^[1-9]{1,9}",
      tipo: "number",
      prefix: {
        value: ""
      }
    },
    {
      etiqueta: "input",
      claseGrid: "col-lg-6 col-md-6 col-sm-6 col-xs-6",
      nombre: "CuentaBanco",
      label_i18n: "cuenta_banco",
      placeholder_i18n: "cuenta_banco",
      requerido: true,
      tipo: "text"
    },
    {
      etiqueta: "input",
      claseGrid: "col-lg-6 col-md-6 col-sm-6 col-xs-6",
      nombre: "NumItems",
      label_i18n: "num_items",
      placeholder_i18n: "num_items",
      requerido: true,
      length: "5",
      pattern: "^[1-9]{1,9}",
      tipo: "number",
      prefix: {
        value: ""
      }
    },
    {
      etiqueta: "select",
      claseGrid: "col-lg-6 col-md-6 col-sm-6 col-xs-6",
      nombre: "TipoImpresion",
      label_i18n: "tipo_impresion",
      placeholder_i18n: "tipo_impresion",
      requerido: true,
      tipo: "text",
      key: "valor",
      opciones: [
        { Id: 1, valor: "Forma continua" },
        { Id: 2, valor: "Hojas normales" }
      ]
    },
    {
      etiqueta: "select",
      claseGrid: "col-lg-6 col-md-6 col-sm-6 col-xs-6",
      nombre: "FormatoImpresion",
      label_i18n: "formato_impresion",
      placeholder_i18n: "formato_impresion",
      requerido: true,
      tipo: "text",
      key: "valor",
      opciones: [
        { Id: 1, valor: "Egreso Generico" },
        { Id: 2, valor: "Formato Adicional" }
      ]
    },
    {
      etiqueta: "input",
      claseGrid: "col-lg-6 col-md-6 col-sm-6 col-xs-6",
      nombre: "NumCopias",
      label_i18n: "num_copias",
      placeholder_i18n: "num_copias",
      requerido: true,
      length: "5",
      pattern: "^[1-9]{1,9}",
      tipo: "number",
      prefix: {
        value: ""
      }
    },
    {
      etiqueta: "input",
      claseGrid: "col-lg-6 col-md-6 col-sm-6 col-xs-6",
      nombre: "Titulo",
      label_i18n: "titulo",
      placeholder_i18n: "titulo",
      requerido: true,
      tipo: "text"
    },
    {
      etiqueta: "select",
      claseGrid: "col-lg-6 col-md-6 col-sm-6 col-xs-6",
      nombre: "NumeracionAutomatica",
      label_i18n: "numeracion_automatica",
      placeholder_i18n: "numeracion_automatica",
      requerido: true,
      tipo: "text",
      key: "valor",
      opciones: [
        { Id: 1, valor: "Si", valorBool: true },
        { Id: 2, valor: "No", valorBool: false }
      ]
    }
  ]
};