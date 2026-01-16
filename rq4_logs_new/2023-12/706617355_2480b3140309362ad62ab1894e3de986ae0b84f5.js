// comments.mjs

import fetch from 'node-fetch';

const API_ENDPOINT = 'http://localhost:3000/comments'; // Replace with your actual API endpoint

// Function to retrieve all comments
export async function getComments() {
  try {
    const response = await fetch(API_ENDPOINT);
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }
    return await response.json();
  } catch (error) {
    console.error('Error fetching comments:', error);
  }
}

// Function to add a new comment
export async function addComment(commentData) {
  try {
    const response = await fetch(API_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(commentData),
    });
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }
    return await response.json();
  } catch (error) {
    console.error('Error adding comment:', error);
  }
}

// Function to update a comment
export async function updateComment(commentId, commentData) {
  try {
    const response = await fetch(`${API_ENDPOINT}/${commentId}`, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(commentData),
    });
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }
    return await response.json();
  } catch (error) {
    console.error('Error updating comment:', error);
  }
}

// Function to delete a comment
export async function deleteComment(commentId) {
  try {
    const response = await fetch(`${API_ENDPOINT}/${commentId}`, {
      method: 'DELETE'
    });
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }
    return await response.json();
  } catch (error) {
    console.error('Error deleting comment:', error);
  }
}
x   