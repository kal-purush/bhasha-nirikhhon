import { Component } from '@angular/core';
import { NavController } from 'ionic-angular';
import { AuthService } from '../../../services/auth-service';
import { ProfilePage } from '../profile/profile';
import { VehicleListPage } from '../../vehicle/list/vehicle-list';
import { UtilProvider } from '../../../providers/util-provider';


@Component({
	selector: 'page-register-user',
	templateUrl: 'register-user.html',
})
export class RegisterUserPage {

	private loading: any;
	private repit: any;
	public User = { empresa_id: 1, nombres: '', apellidos: '', email: '', password: '' };

	constructor(
		public navCtrl: NavController,
		private authService: AuthService,
		private util: UtilProvider
	) { }

	public signUp() {
		let valid = this.validUser();
		if (valid && !this.repit) {
			this.repit = true;
			this.loading = this.util.loading();
			this.authService.signUpAdmin(this.User).subscribe(response => {
				if (response.token) {
					this.util.presentToast("El usuario se registro con éxito");
					this.User = { empresa_id: 1, nombres: '', apellidos: '', email: '', password: '' };
				} else {
					this.util.presentToast(response);
				}
				this.loading.dismiss();
				this.repit = false;
			},
				error => {
					this.util.presentToast(this.util.strings.error_connection);
					this.loading.dismiss();
					this.repit = false;
				});
		} else if(!valid){
			this.util.presentToast("Los campos deben ser mayor a 3 letras");
		}
	}

	public validUser() {
		return this.User.nombres.length > 2 && this.User.apellidos.length > 2 && this.User.email.length > 2 && this.User.password.length > 2;
	}

}