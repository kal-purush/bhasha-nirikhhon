const fs = require("fs");
const path = require("path");

// const data_folder = "./data/";

// Define global arrays to hold data
let posts = [];
let categories = [];

let postIdCounter = 30; // Initialize with the appropriate starting ID

// Function to initialize data by reading JSON files
function initialize() {
  return new Promise((resolve, reject) => {
    // Read posts.json file
    fs.readFile(
      path.join(__dirname, "data", "posts.json"),
      "utf8",
      (err, postData) => {
        if (err) {
          reject("Unable to read posts file");
        } else {
          try {
            posts = JSON.parse(postData);

            // Read categories.json file after successfully reading posts.json
            fs.readFile(
              path.join(__dirname, "data", "categories.json"),
              "utf8",
              (err, categoryData) => {
                if (err) {
                  reject("Unable to read categories file");
                } else {
                  try {
                    categories = JSON.parse(categoryData);
                    resolve("Data initialization successful");
                  } catch (categoryError) {
                    reject("Error parsing categories JSON");
                  }
                }
              }
            );
          } catch (postError) {
            reject("Error parsing posts JSON");
          }
        }
      }
    );
  });
}

// Function to get all posts
function getAllPosts() {
  return new Promise((resolve, reject) => {
    if (posts.length === 0) {
      reject("No results returned");
    } else {
      resolve(posts);
    }
  });
}

// Function to get published posts
function getPublishedPosts() {
  return new Promise((resolve, reject) => {
    const publishedPosts = posts.filter((post) => post.published === true);
    if (publishedPosts.length === 0) {
      reject("No published posts found");
    } else {
      resolve(publishedPosts);
    }
  });
}

function getPostsByCategory(category) {
  return new Promise((resolve, reject) => {
    let postsInCategory = posts.filter((post) => post.category == category);
    if (postsInCategory.length > 0) {
      resolve(postsInCategory);
    } else {
      reject("No posts in that category");
    }
  });
}

function getPostsByMinDate(minDateStr) {
  return new Promise((resolve, reject) => {
    let postsPastDate = posts.filter(
      (post) => new Date(post.postDate) >= new Date(minDateStr)
    );
    if (postsPastDate.length > 0) {
      resolve(postsPastDate);
    } else {
      reject(`No posts past ${minDateStr}`);
    }
  });
}

function getPostById(id) {
  return new Promise((resolve, reject) => {
    const post = posts.find((post) => post.id == id);
    if (post) resolve(post);
    else reject(`No post found with id: ${id}`);
  });
}

// Function to get all categories
function getCategories() {
  return new Promise((resolve, reject) => {
    if (categories.length > 0) {
      resolve(categories);
    } else {
      reject("No categories returned");
    }
  });
}

const addPost = (postData) => {
  const post = {
    ...postData,
    postDate: new Date().toISOString().split("T")[0],
  };
  posts.push(post);
  return post;
};

async function getPublishedPostsByCategory(category) {
  return new Promise((resolve, reject) => {
    let publishedPosts = posts.filter(
      (post) => (post.published == true) & (post.category == category)
    );
    if (publishedPosts.length > 0) {
      resolve(publishedPosts);
    } else {
      reject("No published posts in that category returned");
    }
  });
}

// Export the functions
module.exports = {
  initialize,
  getAllPosts,
  getPublishedPosts,
  getPostsByCategory,
  getPostsByMinDate,
  getPostById,
  getCategories,
  addPost,
  getPublishedPostsByCategory,
};