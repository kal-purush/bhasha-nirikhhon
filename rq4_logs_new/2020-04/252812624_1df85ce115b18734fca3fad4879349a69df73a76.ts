import { Injectable } from '@angular/core';
import { Activitat, User, Video } from 'models/models';
import { AuthService } from 'services/auth/auth.service';

@Injectable({
  providedIn: 'root'
})
export class FiltreRespostesService {

  constructor(
    private auth: AuthService
  ) { }

  obtenirRespostesFiltrades(activitat: Activitat) {

    console.log("activitat", activitat);

    // Si soc professor o no cal validar els videos els puc veure tots
    if (activitat.socProfessor || !activitat.calValidacio) 
      return activitat.videos;
      
    // Si l'activitat es privada nomes puc veure el meu video
    let user: User = this.auth.getUser();
    if (activitat.esPrivada) 
      return activitat.videos.filter(v => v.enviatPer.id == user.id);

    // Si l'activitat no es privada, pero cal validacio del profe
    // nomes puc veure els que ja s'han validat
    if (!activitat.esPrivada && activitat.calValidacio) 
      return activitat.videos.filter(v => v.validat);

    return [];
    
  }

  calValidacio(video: Video, activitat: Activitat) {

    if (!activitat.calValidacio)
      return false;
    
    // Si no soc professor nomes puc veure la validacio de la meva activitat
    let user: User = this.auth.getUser();
    if (!activitat.socProfessor && video.enviatPer.id != user.id)
      return false;

    return true;

  }

}