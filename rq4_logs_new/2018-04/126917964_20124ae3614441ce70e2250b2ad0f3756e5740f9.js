let form_charla = `
<h3>Quiero que den una charla en mi ciudad</h3>
<div class="form_argent" >
	<div class="forms-builder-wrapper" >
		<form class="description" action="https://my.sendinblue.com/users/subscribeembed/js_id/2tksq/id/2" method="POST">
			<input type="hidden" name="js_id" id="js_id" value="2tksq"><input type="hidden" name="listid" id="listid" value="12"><input type="hidden" name="from_url" id="from_url" value="yes"><input type="hidden" name="hdn_email_txt" id="hdn_email_txt" value=""> <input type="hidden" name="sib_simple" value="simple"> <input type="hidden" name="sib_forward_url" value="http://atomiclab.org/argentinaron" id="sib_forward_url">
			<div class="sib-container rounded" >
				<input type="hidden" name="req_hid" id="req_hid" value="~NOMBRE~SURNAME" >
				<div class="view-messages" > </div>
				<!-- an email as primary -->
				<div class="primary-group email-group forms-builder-group ui-sortable" >
					<div class="row" >
						<div class="lbl-tinyltr" >
							Nombre
							<red ></red>
						</div>
						<input required type="text" name="NOMBRE" id="NOMBRE" value="" >
						<div class="clear" ></div>
						<div class="hidden-btns"> <a class="btn move" href="#"><i class="fa fa-arrows"></i></a><br> <a class="btn btn-danger delete" href="#"><i class="fa fa-trash-o fa-inverse"></i></a> </div>
					</div>
					<div class="row" >
						<div class="lbl-tinyltr" >Apellido<span ></span></div>
						<input type="text" name="SURNAME" id="SURNAME" value="" >
						<div class="clear" ></div>
						<div class="hidden-btns"> <a class="btn move" href="#"><i class="fa fa-arrows"></i></a><br> <a class="btn btn-danger delete" href="#"><i class="fa fa-trash-o fa-inverse"></i></a> </div>
					</div>
					<div class="row mandatory-email" >
						<div class="lbl-tinyltr" >Correo electrónico</div>
						<input type="email" name="email" id="email" value="" >
						<div ></div>
						<div class="hidden-btns">
							<a class="btn move" href="#"><i class="fa fa-arrows"></i></a><br> <!--<a class="btn btn-danger delete" href="#"><i class="fa fa-trash-o fa-inverse"></i></a>-->
						</div>
					</div>
				</div>
				<div class="captcha forms-builder-group" >
					<div class="row">
						<div id="gcaptcha" ></div>
					</div>
				</div>
				<!-- end of primary -->
				<div id="recaptcha"></div>
				<div class="byline"> <button class="button editable " type="submit" data-editfield="subscribe">Enviar</button></div>
			</div>
		</form>
	</div>
</div>
`;

let form_sponsors = `
<h3>Ser sponsor</h3>
<div class="form_argent" >
	<div class="forms-builder-wrapper" >
		<form class="description" action="https://my.sendinblue.com/users/subscribeembed/js_id/2tksq/id/3" method="POST">
			<input type="hidden" name="js_id" id="js_id" value="2tksq"><input type="hidden" name="listid" id="listid" value="13"><input type="hidden" name="from_url" id="from_url" value="yes"><input type="hidden" name="hdn_email_txt" id="hdn_email_txt" value=""> <input type="hidden" name="sib_simple" value="simple"> <input type="hidden" name="sib_forward_url" value="http://atomiclab.org/argentinaton" id="sib_forward_url">
			<div class="sib-container rounded" >
				<input type="hidden" name="req_hid" id="req_hid" value="~NOMBRE~SURNAME~EMPRESA" >
				<div class="view-messages" > </div>
				<!-- an email as primary -->
				<div class="primary-group email-group forms-builder-group ui-sortable" >
					<div class="row" >
						<div class="lbl-tinyltr" >
							Empresa
							<red ></red>
						</div>
						<input required type="text" name="EMPRESA" id="EMPRESA" value="" >
						<div class="clear" ></div>
						<div class="hidden-btns"> <a class="btn move" href="#"><i class="fa fa-arrows"></i></a><br> <a class="btn btn-danger delete" href="#"><i class="fa fa-trash-o fa-inverse"></i></a> </div>
					</div>
					<div class="row" >
						<div class="lbl-tinyltr" >
							Nombre
							<red ></red>
						</div>
						<input required type="text" name="NOMBRE" id="NOMBRE" value="" >
						<div class="clear" ></div>
						<div class="hidden-btns"> <a class="btn move" href="#"><i class="fa fa-arrows"></i></a><br> <a class="btn btn-danger delete" href="#"><i class="fa fa-trash-o fa-inverse"></i></a> </div>
					</div>
					<div class="row" >
						<div class="lbl-tinyltr" >
							Apellido
							<red ></red>
						</div>
						<input required type="text" name="SURNAME" id="SURNAME" value="" >
						<div class="clear" ></div>
						<div class="hidden-btns"> <a class="btn move" href="#"><i class="fa fa-arrows"></i></a><br> <a class="btn btn-danger delete" href="#"><i class="fa fa-trash-o fa-inverse"></i></a> </div>
					</div>
					<div class="row mandatory-email" >
						<div class="lbl-tinyltr" >
							<red ><font color="#343434">Correo electrónico laboral</font></red>
							<red ></red>
						</div>
						<input required type="email" name="email" id="email" value="" >
						<div ></div>
						<div class="hidden-btns">
							<a class="btn move" href="#"><i class="fa fa-arrows"></i></a><br> <!--<a class="btn btn-danger delete" href="#"><i class="fa fa-trash-o fa-inverse"></i></a>-->
						</div>
					</div>
				</div>
				<div class="captcha forms-builder-group" >
					<div class="row" >
						<div id="gcaptcha" ></div>
					</div>
				</div>
				<!-- end of primary -->
				<div id="recaptcha"></div>
				<div class="byline" > <button class="button editable " type="submit" data-editfield="subscribe" >Enviar</button></div>
			</div>
		</form>
	</div>
</div>
`;

let form_donaciones = `
<div>
	<h3>Doná al proyecto</h3>
	<div class="form_donaciones">
		<p>Donando a Argentinatón nos ayudás a llegar a más personas en todo el país.</p>
		<div class="donations">
			<div class="donation_item">
				<a target="_blank" href="http://mpago.la/tGgI">
					<p class="km">200<span> kilómetros</span></p>
					<p class="valor">$2.000 (ARS)</p>
				</a>
			</div>
			<div class="donation_item">
				<a target="_blank" href="http://mpago.la/eyjF">
					<p class="km">100<span> kilómetros</span></p>
					<p class="valor">$1.000 (ARS)</p>
				</a>
			</div>
			<div class="donation_item">
				<a target="_blank" href="http://mpago.la/XBAU">
					<p class="km">50<span> kilómetros</span></p>
					<p class="valor">$500 (ARS)</p>
				</a>
			</div>
		</div>
	</div>
</div>
`;

function open_form(name = false, event = false){
	if(event != false){
		if(event.keyCode != 13){
			return false;
		}
	}
	let html = '';
	let captcha = false;
	if(name == 'sponsor'){
		html = form_sponsors;
		captcha = true;
	}
	if(name == 'charla'){
		html = form_charla;
		captcha = true;
	}
	if(name == 'donar'){
		html = form_donaciones;
	}
	$('body').addClass('full_pop_show');
	$('.full_view_output').html(html);
	if(captcha){
		var captchaContainer = null;
		let loadCaptcha = function() {
			captchaContainer = grecaptcha.render('recaptcha', {
				'sitekey' : '6Ld-BFAUAAAAADih-o6oeDjZreC3S5oalMQ2-4g9',
				'callback' : function(response) {
					console.log(response);
				}
			});
		};
		loadCaptcha();
	}
}

function close_full_popup(event = false){
	if(event != false){
		if(event.keyCode != 13){
			return false;
		}
	}
	$('body').removeClass('full_pop_show');
	$('.full_view_output').html('');
}