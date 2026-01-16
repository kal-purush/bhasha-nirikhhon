import {IItem} from '../common/commonInterfaces'


// interface IItem{
//     id: number,
//     name: string,
// }

export function autocomplite(users:IItem[], query: string){
   const userValue = query.toLowerCase().trim();

   if(userValue.match(/[a-zA-Z]/i)){
        const searchUsers = users.filter((user:any) => user.name.toLowerCase().includes(userValue))
        console.log('search', searchUsers)

       return searchUsers
   }

   return null;
}