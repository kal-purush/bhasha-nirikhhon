import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, FormBuilder, Validators } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';
import { UserService } from '../artists/user.service';
import { Disciplines } from '../models/disciplines';
import { User } from '../models/user';
import { LocalStorageService } from '../services/local-storage.service';
import { MustMatch } from '../_helpers/must-match.validator';
import { getUserFromToken } from '../_helpers/tokenHelper';

@Component({
  selector: 'app-purchaser-form',
  templateUrl: './purchaser-form.component.html',
  styleUrls: ['./purchaser-form.component.scss']
})
export class PurchaserFormComponent implements OnInit {
  registerForm: FormGroup;
  submitted: Boolean = false;
  dateReg: RegExp = /^\d{2}[./-]\d{2}[./-]\d{4}$/;
  disciplines: Disciplines[];
  locations: string [] = ['Álava/Araba','Albacete','Alicante','Asturias','Ávila','Badajoz','Baleares',
  'Barcelona','Burgos','Cáceres','Cádiz','Cantabria','Castellón','Ceuta','Ciudad Real','Córdoba',
'Cuenca','Gerona/Girona','Granada','Guadalajara','Guipúzcoa/Gipuzkoa','Huelva','Huesca','Jaén',
'La Coruña/A Coruña','La Rioja','Las Palmas','León','Lérida/Lleida','Lugo','Madrid','Málaga','Melilla',
'Murcia','Navarra','Orense/Ourense','Palencia','Pontevedra','Salamanca','Segovia','Sevilla','Soria',
'Tarragona','Tenerife','Teruel','Toledo','Valencia','Valladolid','Vizcaya/Bizkaia','Zamora','Zaragoza'];

selectDisciplines = new FormControl();
disciplinesValues: string[];
disciplinesString: string;
disciplinesLowerCase: string;


 
  user: User;

  userId: Number;
  token: string;


  constructor(formBuilder: FormBuilder, private userService: UserService, private router: Router,  private route: ActivatedRoute, private lss: LocalStorageService) {
    this.registerForm = formBuilder.group({
      user_name: ['', Validators.required],
      user_id: [''],
      last_name: ['', Validators.required],
      date_of_birth: ['', Validators.required],
      location: ['', Validators.required],
      discipline:[['']],
      mail: ['', [Validators.required, Validators.email]],
      password: ['', [Validators.required, Validators.minLength(4)]],
      confirmPassword: ['', Validators.required],
    }, {
      validator: MustMatch('password', 'confirmPassword')
    });

   }

  ngOnInit(): void {
    this.getDisciplines();
    this.route.params.subscribe(params => {
      if (params.id) {
        this.userService.getUserById(params.id).subscribe(user => {
          if (user) {
            this.user = user;
            this.registerForm.patchValue(user);
          }
          this.userId = params.id;
        });
      }
    });

  }

  getDisciplines(): void {
    this.userService.getDisciplines().subscribe(discipline => this.disciplines = discipline);
  }

  get loginForm() { return this.registerForm.controls }

  onSubmit(obj: any): void {
    this.submitted = true;
    if (this.registerForm.valid) {
      this.registerForm.patchValue({discipline: this.disciplinesLowerCase })
      this.userService.saveUser(this.registerForm.value).subscribe(
        () => this.router.navigate([`/artista/${getUserFromToken().user_id}`]),
        (error) => this.registerForm.setErrors({ userNotFound: error.error })
      )
      }
    };

    clickDiscipline(){
      console.log('entra en click')
      console.log(this.selectDisciplines)
      this.disciplinesValues = this.selectDisciplines.value;
      console.log('estas síiii')
      console.log(this.disciplinesValues.toString())
      this.disciplinesString = this.disciplinesValues.toString();
      this.disciplinesLowerCase = this.disciplinesString.toLowerCase();
      
    }
  

}