import express, { Request, Response, NextFunction, Router } from "express";
import { User, Reserva } from "../models/Users";
import { Properties } from "../models/Properties";

const router = Router();

router.get("/getusers", async (req, res) => {
    const users = await User.find({})
    const userMapped = await Promise.all(users.map(async (e) => {
    const find = await Properties.find({ host: e.email });
      return {
        name: e.name,
        phone: e.phone_number,
        email: e.email,
        lodgings_registered: find.length,
        status_account: e.status_account,
        isModerator: e.isModerator
      }
    }))
    return res.json(userMapped)
  
  })
  
  router.get("/getprops", async (req, res) => {
   
    const find = await Properties.find({host:{$exists:true}});
    const propsMapped = await Promise.all(find.map(async(e) => {
    const nameUser = await User.findOne({email: e.host})
      return {
        name: e.name,
        host: nameUser.name,
        city: e.city,
        reservations_completed: 1,
        status_account: e.status_account,
      }
    }))
    return res.json(propsMapped)
  })
  
  router.put("/updateRolesUser", async (req, res) => {
    const { email,status_account,isModerator} = req.body;
    const user = await User.updateOne({email: email},
    {
      $set: {status_account: status_account,
            isModerator: isModerator,
      }
    }
    )
    res.send("User update!")
  })

export default router;