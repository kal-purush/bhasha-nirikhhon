export class User{


    public firstName: string;
    public lastName: string;
    public sexConstant: number;
    public weight: number;
    public sex: string


    constructor(firstName: string, lastName: string, sex: string, weight: number){
        this.firstName = firstName;
        this.lastName = lastName;
        this.weight = weight;
        this.sex = sex;
        if(sex === "male"){
            this.sexConstant = .68;
        }
        else{
            this.sexConstant = .55;
        }
    }

    

}