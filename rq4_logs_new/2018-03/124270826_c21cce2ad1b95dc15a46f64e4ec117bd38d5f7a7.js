// Practice with Array's and Objects

let movieViews = [
    {
        title: 'Wreck it Ralph',
        rating: '4.5 Star',
        watched: true,
    },
    {
        title:'Ex MAchina',
        rating: '5 Star',
        watched: false,
    },
    {
        title: 'Jurassic Park',
        rating: '5 Star',
        watched: true,
    },
    {
        title: 'Moana',
        rating: '5 Star',
        watched: true,
    },
    {
        title: 'Sin City',
        rating: '5 Star',
        watched: true,
    },
    {
        title: 'Dr. Strange',
        rating: '5 Star',
        watched: false,
    }
];

// a for loop set to go through each object in the array and form a sentence based on whether it has been viewed or not

// for (m = 0; m < movieViews.length; m++) {
//     if (movieViews[m].watched == false) {
//         console.log('You have not seen ' + (movieViews[m].title)+ ', it has been rated as a '+ (movieViews[m].rating)+ ' picture!' );
//     } else {
//     console.log('You have seen ' +(movieViews[m].title) +', You rated it as a ' +(movieViews[m].rating)+ ' picture!');
//     }
// };


// attempt at making better code by using forEach loop (had to do this as a code along as got lost with the buildstat function)
function buildstat(movieViews){ 
    let strin = "You have " ;
    if (movieViews.watched){
        strin += "watched ";
    } else {
        strin += "not seen ";
    }
    strin += "\"" + movieViews.title + "\" - ";
    strin += movieViews.rating +"stars";
    return(strin);
};

movieViews.forEach(function(movieV) {
    console.log(buildstat(movieV));
});