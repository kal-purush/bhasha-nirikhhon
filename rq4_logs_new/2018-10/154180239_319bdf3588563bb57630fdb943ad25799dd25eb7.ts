
import { Injectable } from '@angular/core';
import { HttpClient,HttpHeaders } from '@angular/common/http';
import { UsuariosServiceProvider } from '../usuarios-service/usuarios-service';
import * as moment from "moment";
import { Usuario } from '../../modelos/usuario';
import { Post } from '../../modelos/post';

@Injectable()

export class PostServiceProvider {
  
  private headers: HttpHeaders;
  public usuarios:Usuario[];
  public usuario:Usuario;
  public posts:Post[];
  public post:Post;


  constructor(private _http: HttpClient,
    public usuarioService:UsuariosServiceProvider
    
    ) {
      // Criar as variaveis para autenticação
      this.headers = new HttpHeaders({
        'Content-Type': 'application/json',
        'Authorization':'JWT '+this.usuarioService.token,
      });
      
  
  }
// PORQUE AO INICIAR A PAGINA A FUNÇÂO obtemPius PRECISA ESTAR NA HOME.ts ???
  // obtemPius(){

  //   this._http.get<Post[]>('http://piupiuwer.polijunior.com.br/api/pius/')
  //   .subscribe(
  //     (posts) =>{
  //       moment.locale('pt-BR');
  //       // Trata cada piu e adiciona informação do username e tempo relativo
  //       posts.forEach(post =>{
  //         console.log("olar");
  //         this.adicionaAutor(post);
  //         post.tempoRelativo=moment(post.data).fromNow();
  //         console.log(post.tempoRelativo);
  //       });
  //       this.posts = posts.reverse();
  //       console.log(posts);
        
  //     }
  //   );
                   
  //   }

  // obtemPius(){

  //   this._http.get<Post[]>('http://piupiuwer.polijunior.com.br/api/pius/')
  //   .subscribe(
  //     (posts) =>{
  //       this.posts = posts;
  //       moment.locale('pt-BR');
  //       // Função que adiciona informações relevantes para o card, como tempo relativo e nome de usuario
  //       this.posts.forEach(post => {
  //         this.adicionaAutor(post);
  //         console.log(post.autor);
  //         post.tempoRelativo=moment(post.data).fromNow();

  //       });
  //       console.log(posts);
        
  //     }
  //   );
                   
  //   }


  criaPost(conteudo){
    console.log("entrei na criaPost");
    return new Promise((resolve,reject)=>{
      console.log('SOU O HEADER',this.headers);
      this._http.post('http://piupiuwer.polijunior.com.br/api/pius/',conteudo,{headers:this.headers})
      .subscribe(resposta=>{
        console.log(resposta);
        resolve(resposta);
      }, (erro)=>{
        console.log(erro);
        reject(erro);
      });
  });
  }

  
  adicionaAutor(post){
    this._http.get<Usuario[]>('http://piupiuwer.polijunior.com.br/api/usuarios/')
    .subscribe(
      (usuarios) =>{
        this.usuarios = usuarios;
        // Função que adiciona informações relevantes para o card, como tempo relativo e nome de usuario
        this.usuarios.forEach(usuario => {
          if(usuario.id==post.usuario){
            console.log(usuario.username);
            post.autor=usuario.username;
            console.log("Atribui o autor",post);
          }
        });
      }
    );
  }

}


// RECUPERAÇÂO DA VERSÂO FUNCIONAL
// this._http.post('http://piupiuwer.polijunior.com.br/api/pius/',conteudo)
// .subscribe(resposta=>{
//   resolve(resposta);
// }, (erro)=>{
//   reject(erro);
// });

