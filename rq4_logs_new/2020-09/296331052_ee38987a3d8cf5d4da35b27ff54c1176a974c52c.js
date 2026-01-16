/* eslint-disable quotes */
/* eslint-disable no-console */
/* eslint-disable no-undef */
/* eslint-disable strict */

// `STORE` is responsible for storing the underlying data
// that our app needs to keep track of in order to work.
//
// for a shopping list, our data model is pretty simple.
// we just have an array of shopping list items. each one
// is an object with a `name` and a `checked` property that
// indicates if it's checked off or not.
// we're pre-adding items to the shopping list so there's
// something to see when the page first loads.
const sl = {
  store: {
    items:
      [{ name: "apples", checked: false },
        { name: "oranges", checked: false },
        { name: "milk", checked: true },
        { name: "bread", checked: false },
      ]
  },

  renderShoppingList: function () {
    // this function will be responsible for rendering the shopping list in
    // the DOM
    console.log('`renderShoppingList` ran');
  },

  handleNewItemSubmit: function() {
    // this function will be responsible for when users add a new shopping list item
    console.log('`handleNewItemSubmit` ran');
  },

  handleItemCheckClicked: function() {
    // this function will be responsible for when users click the "check" button on
    // a shopping list item.
    console.log('`handleItemCheckClicked` ran');
  },
  
  handleDeleteItemClicked: function() {
    // this function will be responsible for when users want to delete a shopping list
    // item
    console.log('`handleDeleteItemClicked` ran');
  }
};

// this function will be our callback when the page loads. it's responsible for
// initially rendering the shopping list, and activating our individual functions
// that handle new item submission and user clicks on the "check" and "delete" buttons
// for individual shopping list items.
function handleShoppingList() {
  sl.renderShoppingList();
  sl.handleNewItemSubmit();
  sl.handleItemCheckClicked();
  sl.handleDeleteItemClicked();
}

// when the page loads, call `handleShoppingList`
$(handleShoppingList);