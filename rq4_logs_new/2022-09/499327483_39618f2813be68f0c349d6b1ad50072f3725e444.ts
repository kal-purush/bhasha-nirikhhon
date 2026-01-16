import { firestoreAdmin } from "@libs/firebase/firebaseAdmin";
import { NextApiRequest, NextApiResponse } from "next";

export default async function handler( req: NextApiRequest, res: NextApiResponse )
{
  if( req.method === "POST" ) {
    let uid = req.body.uid;
    
    try {
      let userDocRef = firestoreAdmin.collection( "users" ).doc( uid );
      let userDoc = await userDocRef.get();
      let username = userDoc.data()?.username;
      
      res.status( 200 ).json({ username });
      res.end();
    }
    catch( error ) {
      res.json( error );
      res.status( 405 ).end();
    }
  }
}