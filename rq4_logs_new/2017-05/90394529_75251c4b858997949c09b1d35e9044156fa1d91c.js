    /**
     * Return an object with a next() property. Each time the next function
     * is called the value returned is one higher than the time before.
     *
     *  var c = hw.counter(2);
     *  c.next(); // return 3
     */
    
    function counter() {
        let number = 0; 
        
        return function () {
            return number++;
        };  
    }

let next = counter(); 
next(); 
next(); 
next(); 
next(); 
next(); 
console.log(next()); 


    /**
     * Return a function that accepts the value to multiply `val` by.
     *
     *  let by5 = hw.multiply(5);
     *  by5(2);         // return 10
     *  by5(11);        // return 55
     *  by5(6);         // return 30
     * 
     *  hw.multiply(3)(5); // return 15
     *  hw.multiply(6)(5); // return 30
     */

function multiply (number) {

    return  function (val) {
        return number * val; 
    }; 
}


let by5 = multiply(5); 

console.log(by5(5));  


    /**
     * Return an object with a discount() property. The discount property should
     * accept an amount that the original price should be discounted by. This
     * should not affect the original amount!
     *
     *  var tot = hw.total(20);
     *  tot.discount(0.50); // return 10
     *  tot.discount(0.20); // return 16
     */
 function total (amount) {
 
    return function (discount) {
        return amount * discount; 
    }; 
 }

 let tot = total (10); 
 console.log(tot(.5)); 

     /**
     * Set the name of a user. Only valid names can be provided. A `valid` name is
     * one that matches the regex ^[A-Za-z ]+$.
     *
     *  var user = hw.user(); //function that returns an object
     *  user.setName('Francis Bacon'); // return true
     *  user.getName(); // return 'Francis Bacon'
     *  user.setName('123 hi'); // return false
     *  user.getName(); // return 'Francis Bacon'
     */
    

function valid () {
    let validName = null; 
    return {
        setName: function (name) {
            // console.log(name);
        let regex = new RegExp('^[A-Za-z ]+$'); 
        // console.log(regex.test(name)); 
            if (regex.test(name)) {
                validName = name; 
                return true; 
            } else {
                return false;  
            }
        },
        getName: function () {
            return validName;  
        }
    }; 
}


let user = valid(); 
console.log(user.setName('Francis Bacon')); 
console.log(user.getName()); 

       /**
     * Create a color object that's got six different properties: incrRed(amount), 
     * incrGreen(amount), and incrBlue(amount) - all of which change the R, G, or B
     * value by the specified quantity (could be negative).
     *
     * There should also be a red(), green(), and blue() function that return the current
     * value for that color channel.
     *
     * You can't have a color value less than zero or greater than 255.
     *
     *  var color = hw.color(150, 200, 18);
     *  color.incrRed(12);
     *  color.incrGreen(30);
     *  color.incrBlue(-9);
     *  console.log(color.red(), color.green(), color.blue()); // 162, 230, 9
     */
    // color: function (r, g, b) {},

    function color (r, g, b) {

        return {
            incrRed: function (amount) {
                r = amount + r; 
                if(r < 0) {
                    r = 0; 
                } 
                if (r > 255) {
                    r = 255; 
                }
        },
            incrGreen: function (amount) {
                g = amount + g; 
                if(g < 0) {
                    g = 0; 
                } 
                if (g > 255) {
                    g = 255; 
                }
        },
            incrBlue: function (amount) {
                b = amount + b; 
                if(b < 0) {
                    b = 0; 
                } 
                if (b > 255) {
                    b = 255; 
                }
        },
            red: function () {
                return r; 
        },
            green: function () {
                return g; 
        },
            blue: function () {
                return b; 
        }
    };
}

let col = color(150, 200, 18); 
col.incrRed(12); 
col.incrGreen(30); 
col.incrBlue(-9); 
console.log(col.red(), col.green(), col.blue()); 