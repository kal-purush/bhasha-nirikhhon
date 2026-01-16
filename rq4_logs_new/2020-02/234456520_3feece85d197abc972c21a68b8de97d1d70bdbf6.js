var imageTags = ["image1", "image2", "image3", "image4","image5","image6", "image7", "image8", "image9","image10"];

var actualBlank = 'images/sea2.jpeg';

var actualImages = new Array();


function showBlank()
{
    createRandomImageArray();

    for(var i = 0; i < imageTags.length; i++)
    {

        document.getElementById(imageTags[i]).src= actualBlank;
    }
  }



  function createRandomImageArray()
  {

    const hidImages = [
      'images/sea1.jpg',
      'images/movie7.jpg',
      'images/movie9.jpg',
      'images/sea4.jpg',
      'images/sea5.jpeg'
    ];

      var amount = [0,0,0,0,0];

      while(actualImages.length < 10)
      {
          var randomNumber = Math.floor(Math.random() * hidImages.length)

          if(amount[randomNumber] < 2)
          {
              actualImages.push(hidImages[randomNumber]);
              amount[randomNumber] = amount[randomNumber] + 1;
          }
      }


  }

  function flipImage(number)
  {
      document.getElementById(imageTags[number]).src= actualImages[number];
  }