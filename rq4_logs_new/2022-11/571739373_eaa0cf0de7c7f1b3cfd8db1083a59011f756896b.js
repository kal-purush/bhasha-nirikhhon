const utils 					= require('../utils/utils'); 
const configEmpresaRxServiceClient	= require('../serviceclient/siges-config-empresa/configEmpresaRxService'); 
var handlebars 		= require('handlebars');
var fs 				= require('fs');
//const msgData					= require ('../i18n/messages.json');
/**
 * @description Función que permite  consultar la empresas configuradas
 * @creation David Villanueva 28/11/2022
 * @update
 */
exports.consultarEmpresa= async (req, res) => { 
	 var oResponse	  = {};
	 oResponse.oData  = {}; 
     try { 
	     //Registramos los datos del usuario 
	     var oUsuario			 = {};
		 oUsuario.oAuditRequest = req.headers;
		 oUsuario.oFiltro 		= {};  

		 var cconsultarEmpresaConfigResponse =  await configEmpresaRxServiceClient.consultarEmpresa(oUsuario);
		 if(cconsultarEmpresaConfigResponse.iCode !== 1){
			throw new Error(cconsultarEmpresaConfigResponse.iCode + "||" + cconsultarEmpresaConfigResponse.sMessage);
		 } 
		 console.log(cconsultarEmpresaConfigResponse);

		 oResponse.iCode		= 1;
     	 oResponse.sMessage		= 'OK'; 
     } catch (e) {
        var oError = utils.customError(e);
		if (e.name === 'Error') {
			oResponse.iCode 		= oError.iCode; 
			oResponse.sMessage	= oError.sMessage;
		}else{
			oResponse.iCode 		= -2;
			oResponse.sMessage	=  "Ocurrio un error en el proceso: " + e.toString(); 
		} 
     }finally{
     	oResponse.sIdTransaccion =  req.headers.sidtransaccion;
     	oResponse = utils.customResponse(oResponse);
     }  
     
     res.json(oResponse) 
      
};

 