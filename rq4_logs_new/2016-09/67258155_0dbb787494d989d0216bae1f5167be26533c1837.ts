export class LoginService {
  
  usuarios = [
    { usuarioId: 1, usuario:'admin',  password: 'admin', foto:'C:/cat.jpg' },
    { usuarioId: 2, usuario:'invitado', password: 'invitado', foto:'foto2.jpg' },
    { usuarioId: 3, usuario:'karla',  password: 'karla', foto:'foto3.jpg' },    
  ];
  
  getUsuarios() {
    return this.usuarios;  
  }
  
  getUsuario(id) {
    return this.usuarios.find(usuario => usuario.usuarioId == id);
  }
  
  getUsuarioPassword(username,password) {
    return this.usuarios.find(usuario => ( usuario.usuario == username && usuario.password == password) )  
  }
}