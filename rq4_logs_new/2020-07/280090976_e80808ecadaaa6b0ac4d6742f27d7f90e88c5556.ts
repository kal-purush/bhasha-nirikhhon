export class TrainingsService {
    getTrainings() {
        let trainings =  [
            {
             name: "formation javascript",
             category : "client",
             price : 850
            },
            {
             name: "formation html",
             category : "client",
             price : 650
            },
            {
             name: "formation php",
             category : "serveur",
             price : 950
            },
            {
             name: "formation symfony",
             category : "serveur",
             price : 1050
            }
         
           ];
           
           return trainings;

    }
}