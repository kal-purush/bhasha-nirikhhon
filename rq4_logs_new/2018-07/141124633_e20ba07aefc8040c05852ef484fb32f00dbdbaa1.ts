export class user_format{
    user:string
    pwd:string
    requests:any
    wallet:any
}

export class vehicle_format{
    number:string
    chassis:string
    test:[{
        date:string,
        color:string,
        engine:string,
        odometer:string,
        location:string,
        result:string
    }]
}