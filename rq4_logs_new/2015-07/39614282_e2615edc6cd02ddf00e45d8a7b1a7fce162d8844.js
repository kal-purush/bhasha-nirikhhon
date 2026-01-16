
/* CADASTRO DE USUÁRIO PELO SITE */
	$(document).ready(function(){
		
		$('.dados input').keypress(function() { 
			$(this).removeClass('border_error').addClass('border');
		});
		
		/* icone deleta Frases */
			$('.lista_registros table td.del').click( function () {
				var id = $(this).find("a").attr('rel');
				if(confirm('Excluir esta Categoria? Essa ação não pode ser anulada!')) {
					$.ajax({
						type: "POST",
						url: base_url+"?on=produtosC&in=deletar",
						data: "categoria_id="+$(this).find("a").attr('rel'),
						success: function(msg){
							if(msg == 'ok'){
								$('#'+id).remove();
								alert('Categoria excluido!');
							}else
								alert('Falha ao tentar deletar o usuário, tente novamente mais tarde.');
						}
					});
		       	}
		    });
	    /* icone deleta Frases */
	});
	
	function validaCategoriaProduto(){
		var msg = "";
		
		if($("#categoria").val() == ""){
			msg += "<span>&bull; Categoria não preenchido.</span><br />";
			$("#categoria").addClass('border_error');
		} else
			$("#categoria").removeClass('border_error');
		
		disabledFormButton("input[type='submit']");
			
		if(msg != "") {
			msg = "<span class=\"v11\">Os seguintes campos encontram-se com problemas:<br /><br /></span>" + msg;
			$('#retorno_erro').html(msg);
			
			enabledFormButton("input[type='submit']");
			
			window.clearTimeout(timeoutID);
			timeoutID = window.setTimeout(hideError, 4000, true);
			
		} else {
			
			// VALIDA POR AJAX SE O EMAIL JÁ ESTÁ NA BASE
			$('#retorno_erro').html('Validando...');
			
			$('#cadastro_usuario').submit();
		}

		
	}
	
/* CADASTRO DE USUÁRIO PELO SITE */