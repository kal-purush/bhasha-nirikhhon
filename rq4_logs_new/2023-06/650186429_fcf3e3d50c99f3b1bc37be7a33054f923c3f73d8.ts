import Contest from '<@>/types/contest';
import { NextApiRequest, NextApiResponse } from 'next';

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method === 'POST') {
    // Handle the POST request
    // You can access the request body using req.body
    const {title, description, date, time, link} = req.body;

    // Process the data as needed

    // Return a response
    res.status(200).json({ message: 'POST request successful', title, description, date, time, link});
  } else {
    // Handle other HTTP methods
    res.status(405).json({ message: 'Method Not Allowed' });
  }
}