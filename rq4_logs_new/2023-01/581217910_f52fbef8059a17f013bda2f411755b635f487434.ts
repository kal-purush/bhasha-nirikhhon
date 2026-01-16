// Next.js API route support: https://nextjs.org/docs/api-routes/introduction
import type { NextApiRequest, NextApiResponse } from 'next'
import NextCors from 'nextjs-cors';
import path from 'path';
import { promises as fs } from 'fs';

// type Data = {
//   products: JSON
// }

export default async function handler(
  
  req: NextApiRequest,
  res: NextApiResponse
) {
  await NextCors(req, res, {
    // Options
    methods: ['GET', 'HEAD', 'PUT', 'PATCH', 'POST', 'DELETE'],
    origin: '*',
    optionsSuccessStatus: 200, // some legacy browsers (IE11, various SmartTVs) choke on 204
 });
  //Find the absolute path of the json directory
 const jsonDirectory = path.join(process.cwd(), 'data');
   //Read the json data file data.json
   const fileContents = await fs.readFile(jsonDirectory + '/db.json', 'utf8');
   
   res.status(200).json(JSON.parse(fileContents));
}