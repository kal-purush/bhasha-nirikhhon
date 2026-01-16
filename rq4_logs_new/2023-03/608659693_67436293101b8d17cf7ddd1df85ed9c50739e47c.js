const chapter=Math.floor(Math.random() * 17)
    let url=`https://raw.githubusercontent.com/gita/Datasets/main/chanakya/Chanakya-Niti/chapter${chapter}.json`
    
       export async function fetchData() {
            const response = await fetch(url);
            
            const json = await response.json();
            
            const randomIndex = Math.floor(Math.random() * json.length);
            const randomObject = json[randomIndex];
            return randomObject
        //    console.log(randomObject.text)
           

          }
