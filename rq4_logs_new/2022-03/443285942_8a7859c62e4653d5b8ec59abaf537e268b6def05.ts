interface Passenger {
    name: string;
    sons?: string[]
}

const passenger1: Passenger = {
    name: 'Angel'
}

const passenger2: Passenger = {
    name: 'Catalina',
    sons: ['Katia', 'Andrea']
}

function printSons( passenger: Passenger ): void {
    
    const howManySons = passenger.sons?.length || 0;
    
    console.log( howManySons );
}

printSons( passenger1 );