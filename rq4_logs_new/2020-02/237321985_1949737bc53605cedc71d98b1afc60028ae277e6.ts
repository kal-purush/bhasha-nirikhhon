export class ErrorHandler{
  
  static organizaErro(error){
    switch (error.code) {
      case 'auth/wrong-password':
          return 'Ops Senha incorreta, por favor tente novamente';
      case 'auth/user-not-found':
          return 'Ops Usuario não encontrado, por favor tente novamente';
      case 'auth/invalid-email':
          return 'Ops Email inválido, por favor tente novamente';
      case 'auth/email-already-in-use':
        return 'Ops este email já está em uso';
      case 'auth/weak-password':
        return 'Senha muito fraca, por favor tente outra';
    }
  }
}