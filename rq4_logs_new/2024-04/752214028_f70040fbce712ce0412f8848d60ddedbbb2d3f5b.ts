    import axios from 'axios';
    import { sendNameProps } from '@/app/types/types';
    
export class TrelloapiRepository {  
           async trelloSender({ sendCardName,sendDescName }:sendNameProps) {
            return new Promise(async (resolve, reject) => {
        try {
                const response = await axios.post(
                    `https://api.trello.com/1/cards?key=${process.env.NEXT_PUBLIC_APP_APIKEY}&token=${process.env.NEXT_PUBLIC_APP_APITOKEN}&idList=${process.env.NEXT_PUBLIC_APP_APILIST}`,
                    {
                    method: 'POST',
                    headers: {
                        'Accept': 'application/json'
                    },
                    name:sendCardName,
                    desc:sendDescName
                });
            
                console.log('Card created successfully:', response.data);
                resolve(response.data)
                } catch (error) {
                console.error('Error creating card:', error);
                reject("エラー")
                }
            }  
        )}   
    }