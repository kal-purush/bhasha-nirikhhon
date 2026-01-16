import { Request, Response } from "express";
import { LocationService } from "../services/location.service";
import { UpdatelocationsValidation, locationValidation, locationsValidation } from "../validations/locationValidation";
import { RegionDto } from "../dto/LocationDto";
import * as fs from 'fs';
import csv from 'csv-parser';

class LocationController extends LocationService{
    constructor(){
        super(); //
    }

    createRegions = async(req: Request, res: Response)=>{
        try {
            const { error } = locationValidation(req.body);
            if (error)
                return res.status(400).json({
                    error: true,
                    message: error.details[0].message,
                });
            const data: RegionDto = req.body;
            const result = await this.createRegion(data);
            if(!result)
                return res.status(500).json({
                    status: false,
                    message: 'Something went wrong while creating region',
                })
    
            return res.status(200).json({
                error: false,
                message: "region created",
                data:result
            })
        } catch (error) {
            console.log(error);
            return res.status(500).json({
                error: true,
                message: error
            }); 
        }
    }

    fetchAllRegions = async(req: Request, res: Response) =>{
        try {
            const result = await this.getAllRegion();
            return res.status(200).json({
                error: false,
                message: "region fetched",
                data:result
            })
        } catch (error) {
            console.log(error);
            return res.status(500).json({
                error: true,
                message: error
            }); 
        }
    }

    fetchOneRegion = async(req: Request, res: Response) =>{
        try {
            const result = await this.getOneRegion(req.params.id);
            if(!result){
                return res.status(404).json({
                    error:false,
                    message: "record doesnt exist"
                })
            }
            return res.status(200).json({
                error: false,
                message: "region fetched",
                data:result
            })
        } catch (error) {
            console.log(error);
            return res.status(500).json({
                error: true,
                message: error
            }); 
        }
    }

    updateOneRegion = async(req: Request, res: Response) =>{
        try {
            const { error } = locationValidation(req.body);
            if (error)
                return res.status(400).json({
                    error: true,
                    message: error.details[0].message,
                });
            const data: RegionDto = req.body;
            const id = req.params.id;
            const result = await this.updateRegion(id,data);
            if(!result)
                return res.status(500).json({
                    status: false,
                    message: 'Something went wrong while updating region',
                })
    
            return res.status(200).json({
                error: false,
                message: "region updated",
                data:result
            })
        } catch (error) {
            console.log(error);
            return res.status(500).json({
                error: true,
                message: error
            }); 
        }
    }


    createLocations = async(req: Request, res: Response)=>{
        try {
            const { error } = locationsValidation(req.body);
            if (error)
                return res.status(400).json({
                    error: true,
                    message: error.details[0].message,
                });
            const data: RegionDto = req.body;
            const result = await this.createLocation(data);
            if(!result)
                return res.status(500).json({
                    status: false,
                    message: 'Something went wrong while creating location',
                })
    
            return res.status(200).json({
                error: false,
                message: "location created",
                data:result
            })
        } catch (error) {
            console.log(error);
            return res.status(500).json({
                error: true,
                message: error
            }); 
        }
    }

    fetchAllLocations = async(req: Request, res: Response) =>{
        try {
            const result = await this.getAllLocation();
            return res.status(200).json({
                error: false,
                message: "location fetched",
                data:result
            })
        } catch (error) {
            console.log(error);
            return res.status(500).json({
                error: true,
                message: error
            }); 
        }
    }

    fetchOneLocation = async(req: Request, res: Response) =>{
        try {
            const result = await this.getOneLocation(req.params.id);
            if(!result){
                return res.status(404).json({
                    error:false,
                    message: "record doesnt exist"
                })
            }
            return res.status(200).json({
                error: false,
                message: "location fetched",
                data:result
            })
        } catch (error) {
            console.log(error);
            return res.status(500).json({
                error: true,
                message: error
            }); 
        }
    }

    updateOneLocation = async(req: Request, res: Response) =>{
        try {
            const { error } = UpdatelocationsValidation(req.body);
            if (error)
                return res.status(400).json({
                    error: true,
                    message: error.details[0].message,
                });
            const data: RegionDto = req.body;
            const id = req.params.id;
            const result = await this.updateLocation(id,data);
            if(!result)
                return res.status(500).json({
                    status: false,
                    message: 'Something went wrong while updating locations',
                })
    
            return res.status(200).json({
                error: false,
                message: "region updated",
                data:result
            })
        } catch (error) {
            console.log(error);
            return res.status(500).json({
                error: true,
                message: error
            }); 
        }
    }

    bulkUploadLocation = async (req: Request, res: Response) => {
        try {
            const results: any = [];
        
            fs.createReadStream(req.file!.path)
              .pipe(csv())
              .on('data', (data) => results.push(data))
              .on('end', async () => {
                const importedData = await this.bulkCreate(results);
                res.status(200).json({ message: 'CSV data imported successfully', data: importedData });
              });
          } catch (error) {
            res.status(500).json({ error: 'Error importing CSV data' });
          }
    }
}

export default new LocationController();