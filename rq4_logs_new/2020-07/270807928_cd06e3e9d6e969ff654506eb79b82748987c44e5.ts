import { Request, Response, NextFunction } from "express";


export function corsFilter(req:Request, res:Response, next:NextFunction){
    //we always, on every request options or not, have to set Access-Control-Allow-Origin header
    res.header('Access-Control-Allow-Origin', `${req.headers.origin}`)//this is a dirty hack, its really bad, don't do it when you app is deployed or I will be very disappointed in you
    res.header('Access-Control-Allow-Headers', 'Origin, Content-Type, Accept')
    res.header('Access-Control-Allow-Credentials', 'true')
    res.header('Access-Control-Allow-Methods', 'GET, POST, PATCH, PUT, DELETE')

    //the purpose of an options request is to figure out what kind of requests are allowed to made to the server
    //we specify the kinds of requests using the headers of the response to the options request
    if(req.method === 'OPTIONS'){
        res.sendStatus(200)// will send back the options for pre flight requests
    } else {
        next()//allow the real request to go to the endpoint
    }

}