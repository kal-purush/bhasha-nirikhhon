

const getBackgroundColor = (id) => new Promise((success, fail) =>{
  RequestController.getBackgroundColor(id)
    .then((backgroundColor) => {
     success(backgroundColor); 
    })
    .catch(fail);
});

const onLoad = () => {
  const mainBackground = document.getElementById('background');
  const backgroundColorIds = {1, 2, 3, 4, 5};
  const randomIndex = getRandomIndex(backgroundColorIds);
  
  //TODO: Get the "backgroundColor" value with the "getBackgroundColor" function, and add value in the "mainBackground" HTMLElement
  
}