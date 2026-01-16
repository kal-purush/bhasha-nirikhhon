import randomTime from '@/utils/randomTime'

export default async function(
    fn:(url:string) => Promise<{
        success: boolean;
        data: any;
        numberOfRequest: number;
    }>,
    url:string):Promise<any>{

        let time = randomTime(4, 15)
        let query_results, total_results
        await new Promise(async(resolve, reject) =>{
            const queryInterval = setInterval(async()=>{
                time = randomTime(4, 15)
                const query = await fn(url)
                if(query.success === true){  
                    clearInterval(queryInterval)                  
                    query_results = query.data
                    total_results = query.data?.length

                    console.log("total_results",total_results)
                    resolve({query_results,total_results})
                }else{
                    if(query.numberOfRequest >= 5){
                        reject("exceed request calls")
                    }
                }
            },time)
        })
    }