const obj = {
    name : "Raj Gurjar",
    age: 56,
    add: {
        city: "Gwalior",
        Locality: {
            Colony: "KashiNagar",
            houseNo: "Adv"
        }
    }
}

const person = obj; // This is not shallow copy it is reference passing by.
console.log(obj)
const person2 = {...obj}; // This is shallow copy
// Shallow Copy mein "add" ka sirf reference rahega (Uske Value ka access uske pass nhi hoga).
console.log(person2)


//How to check whether it is a shallow copy or it's Deep Copy.
person2.add.city = "Delhi"
console.log(obj.add.city)
person2.age = 23;
console.log(obj.age)