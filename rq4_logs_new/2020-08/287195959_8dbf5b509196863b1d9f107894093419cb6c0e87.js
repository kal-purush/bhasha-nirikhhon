function orderProblem() {
  let problem = document.getElementById('help-problem');
  let orderText = document.getElementById('orderNumber');
  if (problem.checked === true) {
    orderText.style.display = 'block';
  } else {
    orderText.style.display = 'none';
  }
}

function checkValidate() {
  let form = document.getElementById('signup-form');
  form.onsubmit = function(event) {
    if (!form.checkValidity()) {
      form.classList.add('was-validated');
      event.preventDefault();
      return false;
    }
    return true;
  };
}

window.onload = function() {
  orderProblem();
  checkValidate();
};