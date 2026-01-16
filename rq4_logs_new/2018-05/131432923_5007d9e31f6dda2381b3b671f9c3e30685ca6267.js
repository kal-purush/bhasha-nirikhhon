(() => {
  const config = {
    apiKey: "AIzaSyB8TYS3-6xV76_YhpD0zmYFPq8JtMBXeo8",
    authDomain: "wot-1819.firebaseapp.com",
    databaseURL: "https://wot-1819.firebaseio.com",
    projectId: "wot-1819",
    storageBucket: "wot-1819.appspot.com",
    messagingSenderId: "827619480457"
  };
  firebase.initializeApp(config);

  const firestore = firebase.firestore();

  const matrices = [
    '2eUypCyuAFOY6Kt0rM3k'
  ]
  registerMatricesListeners();

  function registerMatricesListeners() {
    matrices.forEach((matrixId, index) => {
      firestore.collection("matrices").doc(matrixId)
      .onSnapshot({
          // Listen for document metadata changes
          includeMetadataChanges: true
      }, (doc) => {
        const id = doc.id;
        const matrix = {id, ...doc.data()};
        document.querySelector('.matrices-container').innerHTML = generateHtmlForMatrix(matrix);
      });
    });
  }

  function generateHtmlForMatrix(matrix) {
    let tempStr = `<div class="ledmatrix" style="grid-template-columns: repeat(${ matrix.nCols }, 1fr);">`;
    for (let r = 0; r < matrix.nRows; r++) {
      for(let c = 0; c < matrix.nCols; c++) {
        const patternIndex = c + matrix.nRows * r;
        tempStr += `
<div>
  <div class="led" style="background:rgb${matrix.pattern[patternIndex]}">
  </div>
</div>
        `
      }
    }
    return tempStr + '</div>';
  }
})();