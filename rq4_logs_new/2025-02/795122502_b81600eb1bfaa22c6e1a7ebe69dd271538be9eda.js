"use strict"
import Operation from "./general.js"
const token = $('meta[name="csrf-token"]').attr('content')

let btn_search,
btn_save,
edit_id,
select_pais,
select_municipio,
form,
target,
blockUI,
select_lugar,
select_uso,
select_empaque,
select_destinatario,
select_puerto,
select_tefs,
numero_economico,
placas_transporte,
edit_origen,
validations

let obtener_datos = (formulario, pais_id, empaque_id, municipio_id, destinatario_id, puerto_id, tefs_id, lugar_id, uso_id, numero_economico, placas_transporte,  origen) => {
    const clase = "p_input"
    const inputs = formulario.querySelectorAll(`input.${clase}`)
    let datosFormulario = {}

    datosFormulario = {
        pais_id: pais_id,
        empaque_id: empaque_id,
        municipio_id: municipio_id,
        destinatario_id: destinatario_id,
        puerto_id: puerto_id,
        tefs_id: tefs_id,
        lugar_id: lugar_id,
        uso_id: uso_id,
        numero_economico: numero_economico,
        placas_transporte: placas_transporte,
        origen: origen,
        pais_id: pais_id
    }

    Array.from(inputs).forEach(input => {
        switch(input.type) {
            case 'checkbox':
                datosFormulario[input.id] = input.checked
                break
            case 'radio':
                if(input.checked) {
                    datosFormulario[input.name] = input.value
                }
                break
            case 'select-multiple':
                datosFormulario[input.id] = Array.from(input.selectedOptions).map(option => option.value)
                break
            default:
                datosFormulario[input.id] = input.value
        }
    })
    fetch(`save_new_dv_tamplate`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRF-TOKEN': token
        },
        body: JSON.stringify(datosFormulario)
    })
    .then(
        async response => {
            if (!response.ok) {
                const data = await response.json()
                Swal.fire({
                    icon: "error",
                    title: "Error",
                    text: "Ya existe una plantilla para este país si desea modificarla, seleccione el país y presione el botón de buscar ",
                })
            }
            return response.json();
        }
    )
    .then(data => {
        Swal.fire({
            text: "Datos guardados exitosamente!",
            icon: "success",
            buttonsStyling: false,
            confirmButtonText: "Entendido!",
            customClass: {
                confirmButton: "btn btn-primary",
            },
        }).then(({ isConfirmed }) => {
            if (isConfirmed) {
                location.reload()
            }
        })
    })
    .catch((error) => {
        console.error('Error:', error)
    })
    Swal.fire({
        title: "<strong>Cargando</strong>",
        html: `<div class="loader"></div>`,
        showConfirmButton: false,
    })
}
export function init(){
    target = document.querySelector("#kt_block_ui_1_target")
    blockUI = new KTBlockUI(target, {
        overlayClass: "bg-success bg-opacity-15",
        message: '<div class="blockui-message"><span class="spinner-border text-primary"></span> Bloqueado seleccione un pais...</div>'
    })
    btn_save = document.querySelector("#btn_save")
    edit_id = document.querySelector("#plantilla_id")
    btn_search = document.querySelector("#btn_search")
    select_pais = $('#pais_id').select2()
    select_municipio = $("#municipio_id").select2()
    select_lugar = $("#lugar_id").select2()
    select_uso = $("#uso_id").select2()
    select_empaque = $("#empaque_id").select2()
    select_destinatario = $("#destinatario_id").select2()
    select_puerto = $("#puerto_id").select2()
    select_tefs = $("#tefs_id").select2()
    numero_economico = document.querySelector("#numero_economico")
    placas_transporte = document.querySelector("#placas_transporte")
    edit_origen = document.querySelector("#origen_embarque")
    form = document.querySelector("#form_plantilla")
    validations = FormValidation.formValidation(form, {
        fields: {
            lugar_id: {
                validators: {
                    notEmpty: {
                        message: "El lugar es requerido",
                    }
                },
            },
            empaque_id: {
                validators: {
                    notEmpty: {
                        message: "El empaque es requerido",
                    }
                },
            },
            destinatario_id: {
                validators: {
                    notEmpty: {
                        message: "El destinatario es requerido",
                    }
                },
            },
            uso_id: {
                validators: {
                    notEmpty: {
                        message: "El uso es requerido",
                    }
                },
            },
            origen_embarque:{
                validators: {
                    notEmpty: {
                        message: "El origen es requerido",
                    }
                }
            }
        },
        plugins: {
            trigger: new FormValidation.plugins.Trigger(),
            bootstrap: new FormValidation.plugins.Bootstrap5({
                rowSelector: ".fv-row",
                eleInvalidClass: "is-invalid",
                eleValidClass: "is-valid",
            })
        },
    })
   // CHANGE EMPAQUE
    select_empaque.on('change', function() {
        // Operation.get_next_selects("marcas", select_empaque.val(), select_marca)
        Operation.get_next_selects("destinatarios", select_empaque.val(), select_destinatario)
    })

    btn_save.addEventListener("click", function (t) {
        t.preventDefault()
        if(select_pais.val() != '' && select_pais.val() !== null){
            validations &&
                validations.validate().then(function (e) {
                    "Valid" == e
                        ?
                        obtener_datos(form, select_pais.val(), select_empaque.val(), select_municipio.val(), select_destinatario.val(), select_puerto.val(),  select_tefs.val(), select_lugar.val(), select_uso.val(), numero_economico.value, placas_transporte.value, edit_origen.value)
                        : Swal.fire({
                                text: "Error, faltan algunos datos, intente de nuevo por favor.",
                                icon: "error",
                                buttonsStyling: !1,
                                confirmButtonText: "Entendido!",
                                customClass: {
                                    confirmButton: "btn btn-primary",
                                },
                            })
                })
            }
        else{
            Swal.fire({
                icon: "error",
                title: "Error",
                text: "Selecciona un pais",
            })
        }
    })

    btn_search.addEventListener("click", function (t) {
        if(select_pais.val() != '' && select_pais.val() !== null && select_municipio.val() != '' && select_municipio.val() !== null){
            fetch(`get_plantilla/${select_pais.val()}/${select_municipio.val()}`, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRF-TOKEN': token
                },
            })
            .then(response => response.json())
            .then(data => {
                const datos = data.plantilla[0]
                    edit_id.value = datos.id
                    delete datos.id
                    delete datos.created_at
                    delete datos.deleted_at
                    delete datos.updated_at

                    for (const [key, value] of Object.entries(datos)) {
                        const elementos = document.getElementsByName(key);
                        if (elementos.length > 0) {
                            if (elementos[0].type === 'radio') {
                            elementos.forEach(radio => {
                                radio.checked = radio.value == value;
                            });
                            } else if (elementos[0].type === 'checkbox') {
                            elementos[0].checked = Boolean(value);
                            } else {
                            elementos[0].value = value;
                            }
                        }
                    }
                    btn_save.classList.remove("d-none")
                    blockUI.release()
                    Swal.close()
            })
            .catch((error) => {
                console.error('Error:', error)
            })
            Swal.fire({
                title: "<strong>Cargando</strong>",
                html: `<div class="loader"></div>`,
                showConfirmButton: false,
            })
        }
        else{
            Swal.fire({
                icon: "error",
                title: "Error",
                text: "Selecciona un pais y un lugar",
            })
        }

    })
    // CHANGE PAIS
    select_pais.on('change', function() {
        select_municipio.prop('disabled', false)
    })
    blockUI.block()
}

document.addEventListener("DOMContentLoaded", function () {
    init()
})
