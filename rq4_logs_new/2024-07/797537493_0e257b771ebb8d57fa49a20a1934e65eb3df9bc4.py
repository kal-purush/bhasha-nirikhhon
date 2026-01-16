from flask import Blueprint, jsonify
from ..models.contract import Contract
from ..models.content_contract import Contenido_Contrato
from .. import db

contract_by_rut = Blueprint('contract_by_rut', __name__)

@contract_by_rut.route('/get_contract_by_rut/<status>', methods=['GET'])
def get_contracts(status):
    try:
        contracts = db.session.query(
            Contract,
            Contenido_Contrato.nombres,
            Contenido_Contrato.apellidos,
            Contenido_Contrato.direccion,
            Contenido_Contrato.estado_civil,
            Contenido_Contrato.fecha_nacimiento,
            Contenido_Contrato.rut,
            Contenido_Contrato.mail,
            Contenido_Contrato.nacionalidad,
            Contenido_Contrato.sistema_salud,
            Contenido_Contrato.afp,
            Contenido_Contrato.nombre_empleador,
            Contenido_Contrato.rut_empleador,
            Contenido_Contrato.cargo,
            Contenido_Contrato.fecha_inicio,
            Contenido_Contrato.fecha_final,
            Contenido_Contrato.indefinido,
            Contenido_Contrato.sueldo_base,
            Contenido_Contrato.asignacio_colacion,
            Contenido_Contrato.bono_asistencia
        ).join(Contenido_Contrato, Contenido_Contrato.id_contrato == Contract.id_contrato)\
         .filter(Contract.estado == status).all()

        return jsonify([
            {
                'id': contract.id_contrato,
                'estado': contract.estado,
                'contrato': contract.contrato,
                'comentario': contract.comentario,
                'revision_gerente': contract.revision_gerente,
                'revision_gerente_general': contract.revision_gerente_general,
                'nombres': contenido_nombres,
                'apellidos': contenido_apellidos,
                'direccion': contenido_direccion,
                'estado_civil': contenido_estado_civil,
                'fecha_nacimiento': contenido_fecha_nacimiento,
                'rut': contenido_rut,
                'mail': contenido_mail,
                'nacionalidad': contenido_nacionalidad,
                'sistema_salud': contenido_sistema_salud,
                'afp': contenido_afp,
                'nombre_empleador': contenido_nombre_empleador,
                'rut_empleador': contenido_rut_empleador,
                'cargo': contenido_cargo,
                'fecha_inicio': contenido_fecha_inicio,
                'fecha_final': contenido_fecha_final,
                'indefinido': contenido_indefinido,
                'sueldo_base': contenido_sueldo_base,
                'asignacio_colacion': contenido_asignacio_colacion,
                'bono_asistencia': contenido_bono_asistencia
            } for
            contract, contenido_nombres, contenido_apellidos, contenido_direccion, contenido_estado_civil, contenido_fecha_nacimiento, contenido_rut, contenido_mail, contenido_nacionalidad, contenido_sistema_salud, contenido_afp, contenido_nombre_empleador, contenido_rut_empleador, contenido_cargo, contenido_fecha_inicio, contenido_fecha_final, contenido_indefinido, contenido_sueldo_base, contenido_asignacio_colacion, contenido_bono_asistencia
            in contracts
        ]), 200
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500