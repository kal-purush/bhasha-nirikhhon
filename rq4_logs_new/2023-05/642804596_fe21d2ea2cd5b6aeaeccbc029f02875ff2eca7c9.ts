



export interface passenger{
    name : string;
    children?: string[];
}

const passenger1: passenger ={
    name: "Warner"
}

const passenger2: passenger ={
    name: "Raimer",
    children:['keider','randel']

}

// const printChildren = (Passenger:passenger)=>{
    const returnChildrenNumber = (Passenger:passenger):number =>{

     const howManyChildren= Passenger.children?.length || 0;
   // const howManyChildren= Passenger.children!.length;

    console.log(Passenger.name, howManyChildren);

    return howManyChildren;
}
//returnChildrenNumber(passenger1)
returnChildrenNumber(passenger2)