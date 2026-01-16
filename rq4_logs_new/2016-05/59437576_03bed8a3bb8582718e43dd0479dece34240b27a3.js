new Vue({






        el: '#app',
        data: {

            //1= male, 2=female

            sex: '',
		feet: '',
		inch: '',
		lbs: '',
		ActLevel: '',
		age: ''
//    public centimeters: number;
//    public weight: number;
//    public ActivityLevel: number;
//
//
//
//
//    //Macros
//    public starch: number;
//    public sugar: number;
//    public protien: number;
//    public fat: number;
//    public water: number;
//
//
//    //Vitamins
//
//    public VA: number;
//    public VC: number;
//    public VE: number;
//    public sodium: number;
//    public fiber: number;
//
//
//    //personal
//    public location: string;
        },



        computed:{

        cal: function(){
        var act = 0;
        switch(this.ActLevel) {
            case 1:
                act = 1.2;
            break;
            case 2:
            act = 1.55
             break;
            case 3:
            act = 1.7
            break;
            default:
            act = 0;
}


        var cal = 0;

        var cm = ( this.feet   +  (this.inch / 12) ) * 30.48;

        if(this.sex === "Female"){

            console.log(cm);

            cal =  10 * (this.lbs * .453592) + 6.25 * cm - 5 * this.age - 161 * act;
            cal = cal * act;
        }
        if(this.sex === "Male"){

            cal =  10 * (this.lbs * .453592) + 6.25 * cm - 5 * this.age + 5 * act;
            cal = cal * act;
        }

        if(cal > 1000)
            return Math.round(cal) + " calories";
        }


        }



    })