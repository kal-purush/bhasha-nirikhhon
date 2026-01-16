/**
 * @module Main
 * @description Orchestrates the Kanban app by connecting modules and setting up event listeners.
 */

import { fetchTasksFromAPI, loadTasks, getTasks, updateTasks } from './storage.js';
import { renderTasks } from './render.js';
import { openModal, closeModal, saveChanges, deleteTask } from './modal.js';
import { updateLogo, toggleTheme, toggleSidebar } from './sidebar.js';

// DOM elements
const modal = document.getElementById('task-modal');
const closeModalBtn = document.getElementById('close-modal');
const saveChangesBtn = document.getElementById('save-changes');
const deleteTaskBtn = document.getElementById('delete-task');
const newTaskBtn = document.getElementById('new-task-btn');
const themeToggle = document.getElementById('theme-toggle');
const logo = document.getElementById('logo');
const loadingMessage = document.getElementById('loading-message');
const errorMessage = document.getElementById('error-message');
const sidebar = document.getElementById('side-bar-div');
const layout = document.getElementById('layout');
const hideSidebarBtn = document.querySelector('.hide-sidebar');
const showSidebarBtn = document.querySelector('.show-sidebar');
const closeSidebarMobileBtn = document.querySelector('.close-sidebar-mobile');
const logoMobile = document.querySelector('.logo-mobile');

// Initialize theme on page load
document.addEventListener('DOMContentLoaded', () => {
  const savedTheme = localStorage.getItem('theme');
  if (savedTheme === 'dark') {
    document.body.classList.add('dark-mode');
    themeToggle.checked = true;
  } else {
    document.body.classList.remove('dark-mode');
    themeToggle.checked = false;
  }
  updateLogo(logo);

  // Initial render of tasks
  const tasks = loadTasks();
  renderTasks(tasks, openModal);

  // Fetch tasks from API
  fetchTasksFromAPI(loadingMessage, errorMessage).then(() => {
    const updatedTasks = getTasks();
    renderTasks(updatedTasks, openModal);
  });

  // Event listeners
  closeModalBtn.addEventListener('click', closeModal);
  saveChangesBtn.addEventListener('click', () => {
    const tasks = getTasks();
    saveChanges(tasks, updateTasks, renderTasks);
  });
  deleteTaskBtn.addEventListener('click', () => {
    const tasks = getTasks();
    deleteTask(tasks, updateTasks, renderTasks);
  });
  newTaskBtn.addEventListener('click', () => openModal(null, false));
  themeToggle.addEventListener('change', (e) => toggleTheme(e, logo));
  hideSidebarBtn.addEventListener('click', () => toggleSidebar(sidebar, layout, hideSidebarBtn, showSidebarBtn, closeSidebarMobileBtn));
  showSidebarBtn.addEventListener('click', () => toggleSidebar(sidebar, layout, hideSidebarBtn, showSidebarBtn, closeSidebarMobileBtn));
  closeSidebarMobileBtn.addEventListener('click', () => toggleSidebar(sidebar, layout, hideSidebarBtn, showSidebarBtn, closeSidebarMobileBtn));
  logoMobile.addEventListener('click', () => toggleSidebar(sidebar, layout, hideSidebarBtn, showSidebarBtn, closeSidebarMobileBtn));

  // Initial sidebar button visibility for mobile
  if (window.innerWidth <= 1023) {
    showSidebarBtn.style.display = 'block';
  }
});