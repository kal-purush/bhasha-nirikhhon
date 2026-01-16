export function animalPrinter(selector) {
    var birdPicture = `__
    /    \
   / / @ / @ \ \
   \ _/_/ /\
    _/__/||
    /     /\\//
   |     |\\\
    \      \\\
      ___/\\
       ||||_`;

    var squirrelPicture = ` 
    (\__/)  .~    ~. ))
    /O O  ./      .'
   {O__,   \    {
     / .  . )    \
     |-| '-' \    }
    .(   _(   )_.'
   '---.~_ _ _&`;
   
    if(selector == 1) {
       console.log(birdPicture); 
    } else if (selector ==2) { 
        console.log(squirrelPicture);
    } else {
        console.log("wrong animal dingus");
    }
}