const createAutoComplete = ({
  root,
  renderOption,
  onOptionSelect,
  inputValue,
  fetchData
}) => {
  // html template
  root.innerHTML = `
  <label><b>Search</b></label>
  <input class="input" />
  <div class="dropdown">
    <div class="dropdown-menu">
      <div class="dropdown-content results"></div>
    </div>
  </div>
  `;

  // Select input
  const input = root.querySelector('input');
  // render content inside html template
  const dropdown = root.querySelector('.dropdown');
  const resultsWrapper = root.querySelector('.results');

  const onInput = async event => {
    const items = await fetchData(event.target.value);

    // If there's no item/results dont show anything
    if (!items.length) {
      dropdown.classList.remove('is-active');
      return;
    }

    // clear input
    resultsWrapper.innerHTML = '';

    // Add active to html template
    dropdown.classList.add('is-active');
    // Iterate item
    for (let item of items) {
      const option = document.createElement('a');

      // Create dropdown-item class
      option.classList.add('dropdown-item');

      // insert html in option<a></a>
      option.innerHTML = renderOption(item);

      // detect a click on the <a></a> or item
      option.addEventListener('click', () => {
        // close the dropdown
        dropdown.classList.remove('is-active');

        // update text inside the input with title of item clicked
        input.value = inputValue(item);

        // Show the movie and info
        onOptionSelect(item);
      });

      // take div and innerHTML and insert to DOM with id item appendChild
      resultsWrapper.appendChild(option);
    }
  };

  // add event listener to input and listen for input
  input.addEventListener('input', debounce(onInput, 500));

  // Close sidebar menu if use clicks out of the box of list
  document.addEventListener('click', event => {
    // console.log(event.target);

    // if the root/menu/dropdown is not clicked
    if (!root.contains(event.target)) {
      // remove active class that we created above
      dropdown.classList.remove('is-active')
    }
  });
};