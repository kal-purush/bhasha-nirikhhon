interface Chauffeur {
    name: string;
    address: string;
    email: string;
    password: string;
    payments: {
      cardNumber: string;
      expirationDate: string;
      securityCode: string;
    };
    history: {
      orders: string[];
      travels: string[];
    };
    img: string;
    DNI: string;
    license: string;
    vehicle: {
        img: string;
        patent: string;
        brand: string;
        model: string;
        year: string;
    }
  }
  
  export default Chauffeur