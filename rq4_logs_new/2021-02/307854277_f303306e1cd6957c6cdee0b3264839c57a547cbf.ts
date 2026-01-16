const API=require("./API.ts")
const user = 2

API.newGroup(user, "Phantom Blood", "Dio's a jerk part 1")
API.newGroup(user, "Battle Tendancy", "Cool Aztec Vampires")
API.newGroup(user, "Stardust Cruasders", "Some can't stand it")
API.newGroup(user, "Diamond is Unbreakable", "Shonen Sitcom")
API.newGroup(user, "Vento Auero", "Ganster Mommy with confused kids")
API.newGroup(user, "Stone Ocean", "David Bowe Part 2: electric boogalo")
API.newGroup(user, "Steel ball Run", "Most evil Jojo")

API.addmembers(user, 1, 1)
API.addmembers(user, 1, 2)
API.addmembers(user, 1, 3)
API.addmembers(user, 1, 4)

API.addmembers(user, 2, 2)
API.addmembers(user, 2, 3)
API.addmembers(user, 2, 4)
API.addmembers(user, 2, 5)
               user 
API.addmembers(user, 3, 3)
API.addmembers(user, 3, 4)
API.addmembers(user, 3, 5)
API.addmembers(user, 3, 6)
               user  
API.addmembers(user, 4, 4)
API.addmembers(user, 4, 5)
API.addmembers(user, 4, 6)
API.addmembers(user, 4, 7)
/*
.then(res=>console.log(res))
*/

/*
API.getGroup(user)
.then(res=>{
        console.log(res.data.groupmembers)
})


*/