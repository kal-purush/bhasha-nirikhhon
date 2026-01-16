// Next.js API route support: https://nextjs.org/docs/api-routes/introduction
import type { NextApiRequest, NextApiResponse } from "next";
import { createClient } from '@supabase/supabase-js'
type GeneratedImage = {
  imageURL: string;
};
//    YAKUMWAMBA@NFT$
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<GeneratedImage>
) {

    const supabase = createClient('https://lqmvvslhcfindyifblyk.supabase.co', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxxbXZ2c2xoY2ZpbmR5aWZibHlrIiwicm9sZSI6ImFub24iLCJpYXQiOjE2Njk0MjY3ODUsImV4cCI6MTk4NTAwMjc4NX0.ElePLMRZGn4pkHac0ZQj7AfHnGXWGBLaQbs_uyJ9pW0')

    //curl https://api.openai.com/v1/images/generations \
//   -H 'Content-Type: application/json' \
//   -H "Authorization: Bearer $OPENAI_API_KEY" \
//   -d '{
//     "prompt": "a white siamese cat",
//     "n": 1,
//     "size": "1024x1024"
//   }'

    if (req.method === 'POST') {
        // Process a POST request
        console.log(req.body);
        const { data, error } = await supabase.storage.createBucket('avatars')


            const imageResp = await fetch('https://api.openai.com/v1/images/generations', {

                method: 'POST',
                body: JSON.stringify({
                    prompt: req.body.promptText,
                    n: 1,
                    size: '1024x1024'
                }),
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer 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',
                }



        })

        const image = await imageResp.json();



        console.log(image.data[0].url);
        let blob = await fetch(image.data[0].url)
        let blobData = await blob.blob()
        let uuid = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
        const resp = await supabase.storage

        .from('generative-art')
        .upload(`public/${uuid}.png`,    blobData)

        //create a signed url for the image


        console.log("https://lqmvvslhcfindyifblyk.supabase.co/storage/v1/object/", resp.data?.path);
        res.status(200).json({ imageURL: image.data[0].url });

        //https://lqmvvslhcfindyifblyk.supabase.co/storage/v1/object/public/generative-art/public/y1izesg4z2v6udmpwu4h.png
      } else {
        // Handle any other HTTP method
        res.status(200).json({ imageURL: "Get request detected" });
      }

}