import * as services from '../services/roomService'

    //create a Room
    export const createRoom = async (req, res) => {
        try {
            
            const reqBody = req.body
    
            console.log(reqBody)
    
             //check for existing room
            const checkRoom = await services.fetchOne({ name: reqBody.name })
    
            if(checkRoom) res.status(403).json({success: false, message: 'Room Already Exists'})
    
            //else create a room
            const newRoom = await services.Create(reqBody)
    
            res.status(201).json({ success: true, message: 'Room created Successfully', data: newRoom })
        } catch (error) {
            
        }
    }

    //update a Room
    export const updateRoom = async (req, res, next) => {
        try {
            
            const roomId = req.params.id
            const updateData = req.body
            //check if room exits before updating
            const checkRoom = await services.fetchOne({ _id : roomId })
    
            if(!checkRoom) res.status(403).json({ success: false, message: 'Room to update not found'})
    
            //make the unique key consistent
            if(updateData.name){
                const checkRoomUpdate = await services.fetchOne({ name: updateData.name})
    
                if(checkRoomUpdate._id.toString() !== checkRoomUpdate._id.toString()){
                    res.status(403).json({ success: false, message: 'Room name already exists'})
    
                }
            }
    
            //update room
            const updatedData = await services.Update(roomId, updateData)
            res.status(200).json({ success: true, message: 'Room updated successfully', data: updatedData })
        } catch (error) {
            next(error)
        }
    }

    //delete a Room
    export const deleteRoom = async (req, res, next) => {
      
    try {
     const deletedRoom = await services.Delete(req.params.id)
    
    res.status(200).json({ success: true, message: 'Room deleted successfully', data: deletedRoom })
            
    } catch (error) {
            next(error)
        }
    }


    //fetch a single Room
    export const fetchOneRoom = async (req, res, next) => {

        try {
            
        const roomId = req.params.id
        //check if room exits before updating
         const checkRoom = await services.fetchOne({ _id: roomId })
    
        if(!checkRoom) res.status(403).json({ success: false, message: 'Room to fetch not found' })
    
        res.status(200).json({ success: true, message: 'Room fetched successfully', data: checkRoom })
        } catch (error) {
        next(error)
        }
    }

    //Fetch all Rooms
    export const  fetchAllRoom = async(req, res, next) => {
        try {
            
    const search = req.query.search
    const fetchedBooks = await fetch(search)
       
    res.status(200).json({ success: true, message: 'Rooms fetched successfully', data: fetchedBooks })
    } 
    catch (error) {
        next(error)
        }
    }
