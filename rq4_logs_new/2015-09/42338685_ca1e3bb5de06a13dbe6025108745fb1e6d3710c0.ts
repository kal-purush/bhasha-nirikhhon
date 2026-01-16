
export class AuthService {
    token = undefined;
    baseUrl = 'http://localhost:3000/';
    loginUrl = this.baseUrl + 'login'; 
    headers =  {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    };   
    
    
    constructor() {
        
    }
    
    login(username, password){
    /*  return fetch(this.loginUrl, {
        method: 'POST',
        headers: this.headers,
        body: JSON.stringify({
          username, password
        })
      })
      .then((response) => {
        return response.text();
      })
      .then((text) => {
        this.token = JSON.parse(text).id_token;
        localStorage.setItem('jwt', this.token);
      });
      */
      
      return true;
    }
    
}