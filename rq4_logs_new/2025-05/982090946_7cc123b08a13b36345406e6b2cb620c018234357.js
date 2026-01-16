// Estado de la aplicación
const state = {
    tasks: [],
    filter: 'all',
    categoryFilter: 'all',
    priorityFilter: 'all',
    darkMode: false
};

// Referencias a elementos del DOM
const taskForm = document.getElementById('task-form');
const taskList = document.getElementById('task-list');
const filterButtons = document.querySelectorAll('.filter-btn');
const categoryFilter = document.getElementById('category-filter');
const priorityFilter = document.getElementById('priority-filter');
const themeToggle = document.getElementById('theme-toggle');
const tabs = document.querySelectorAll('.tab');
const tabContents = document.querySelectorAll('.tab-content');
const saveBtn = document.getElementById('save-btn');
const loadBtn = document.getElementById('load-btn');

// Charts
let weeklyChart;
let monthlyChart;

// --- Helper Functions ---
function getPriorityText(priorityValue) {
    switch (priorityValue) {
        case 'low': return 'Baja';
        case 'medium': return 'Media';
        case 'high': return 'Alta';
        default: return priorityValue.charAt(0).toUpperCase() + priorityValue.slice(1);
    }
}

function formatDateTime(dateTimeString) {
    if (!dateTimeString) return '';
    try {
        const date = new Date(dateTimeString);
        if (isNaN(date.getTime())) {
            return dateTimeString;
        }
        return date.toLocaleString('es-ES', {
            year: 'numeric', month: 'short', day: 'numeric',
            hour: '2-digit', minute: '2-digit'
        });
    } catch (e) {
        return dateTimeString;
    }
}

function getDaysOfWeek() {
    const dayNames = ['Domingo', 'Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado'];
    const current = new Date();
    const firstDayOfWeek = new Date(current);
    firstDayOfWeek.setDate(current.getDate() - current.getDay()); // Sunday
    const weekLabels = [];
    for (let i = 0; i < 7; i++) {
        const d = new Date(firstDayOfWeek);
        d.setDate(firstDayOfWeek.getDate() + i);
        weekLabels.push(dayNames[d.getDay()]);
    }
    return weekLabels;
}

function getDaysOfMonth() {
    const today = new Date();
    const year = today.getFullYear();
    const month = today.getMonth();
    const daysInMonth = new Date(year, month + 1, 0).getDate();
    const result = [];
    for (let i = 1; i <= daysInMonth; i++) {
        result.push(i.toString());
    }
    return result;
}

// --- Local Storage ---
function saveTasksToLocalStorage() {
    const tasksToSave = state.tasks.map(task => ({
        ...task,
        createdAt: task.createdAt instanceof Date ? task.createdAt.toISOString() : task.createdAt,
    }));
    localStorage.setItem('tasks', JSON.stringify(tasksToSave));
}

function loadTasksFromLocalStorage() {
    const savedTasks = localStorage.getItem('tasks');
    if (savedTasks) {
        try {
            const parsedTasks = JSON.parse(savedTasks);
            state.tasks = parsedTasks.map(task => ({
                ...task,
                createdAt: task.createdAt ? new Date(task.createdAt) : null,
            }));
        } catch (error) {
            console.error("Error loading tasks from localStorage:", error);
            state.tasks = [];
        }
    }
}


// --- Inicialización ---
document.addEventListener('DOMContentLoaded', () => {
    if (localStorage.getItem('darkMode') === 'true') {
        state.darkMode = true;
        document.body.classList.add('dark-mode');
        themeToggle.textContent = '☀️';
    } else {
        state.darkMode = false;
        themeToggle.textContent = '🌙';
    }

    loadTasksFromLocalStorage();
    initCharts();
    renderTasks();
    updateStats();
    updateCharts();
});

document.addEventListener('themeChanged', updateChartThemeColors);


// --- Event Listeners ---
taskForm.addEventListener('submit', (e) => {
    e.preventDefault();
    addTask();
});

filterButtons.forEach(button => {
    button.addEventListener('click', () => {
        filterButtons.forEach(btn => btn.classList.remove('active'));
        button.classList.add('active');
        state.filter = button.dataset.filter;
        renderTasks();
    });
});

categoryFilter.addEventListener('change', () => {
    state.categoryFilter = categoryFilter.value;
    renderTasks();
});

priorityFilter.addEventListener('change', () => {
    state.priorityFilter = priorityFilter.value;
    renderTasks();
});

themeToggle.addEventListener('click', toggleDarkMode);

tabs.forEach(tab => {
    tab.addEventListener('click', () => {
        tabs.forEach(t => t.classList.remove('active'));
        tabContents.forEach(tc => tc.classList.remove('active'));

        tab.classList.add('active');
        const activeTabContentId = `${tab.dataset.tab}-chart`;
        document.getElementById(activeTabContentId).classList.add('active');
    });
});

saveBtn.addEventListener('click', saveTasksToFile);
loadBtn.addEventListener('click', () => {
    const fileInput = document.createElement('input');
    fileInput.type = 'file';
    fileInput.accept = '.json';
    fileInput.style.display = 'none';

    fileInput.addEventListener('change', (e) => {
        const file = e.target.files[0];
        if (file) {
            loadTasksFromFile(file);
        }
        document.body.removeChild(fileInput);
    });

    document.body.appendChild(fileInput);
    fileInput.click();
});

document.body.addEventListener('dragover', (e) => {
    e.preventDefault();
    e.stopPropagation();
    document.body.classList.add('drag-over');
});

document.body.addEventListener('dragleave', (e) => {
    e.preventDefault();
    e.stopPropagation();
    document.body.classList.remove('drag-over');
});

document.body.addEventListener('drop', (e) => {
    e.preventDefault();
    e.stopPropagation();
    document.body.classList.remove('drag-over');

    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
        const file = e.dataTransfer.files[0];
        if (file && file.name.endsWith('.json')) {
            loadTasksFromFile(file);
        } else {
            alert('Por favor, suelta un archivo JSON válido.');
        }
        e.dataTransfer.clearData();
    }
});

// --- Funciones ---
function addTask() {
    const titleInput = document.getElementById('title');
    const categoryInput = document.getElementById('category');
    const priorityInput = document.getElementById('priority');
    const dueDateInput = document.getElementById('due-date');
    const descriptionInput = document.getElementById('description');

    if (!titleInput.value.trim()) {
        alert("El título de la tarea es obligatorio.");
        return;
    }

    const newTask = {
        id: Date.now().toString(),
        title: titleInput.value.trim(),
        description: descriptionInput.value.trim(),
        category: categoryInput.value,
        priority: priorityInput.value,
        dueDate: dueDateInput.value || null,
        completed: false,
        createdAt: new Date()
    };

    state.tasks.unshift(newTask);
    saveTasksToLocalStorage();
    renderTasks();
    updateStats();
    updateCharts();

    taskForm.reset();
    titleInput.focus();
}

function toggleTaskComplete(taskId) {
    const taskIndex = state.tasks.findIndex(task => task.id === taskId);
    if (taskIndex !== -1) {
        state.tasks[taskIndex].completed = !state.tasks[taskIndex].completed;
        if(state.tasks[taskIndex].completed) {
            state.tasks[taskIndex].completedAt = new Date();
        } else {
            delete state.tasks[taskIndex].completedAt;
        }
        saveTasksToLocalStorage();
        renderTasks();
        updateStats();
        updateCharts();
    }
}

function deleteTask(taskId) {
    if (confirm('¿Estás seguro de que deseas eliminar esta tarea?')) {
        state.tasks = state.tasks.filter(task => task.id !== taskId);
        saveTasksToLocalStorage();
        renderTasks();
        updateStats();
        updateCharts();
    }
}

function renderTasks() {
    taskList.innerHTML = '';

    const filteredTasks = state.tasks.filter(task => {
        if (state.filter === 'pending' && task.completed) return false;
        if (state.filter === 'completed' && !task.completed) return false;
        if (state.categoryFilter !== 'all' && task.category !== state.categoryFilter) return false;
        if (state.priorityFilter !== 'all' && task.priority !== state.priorityFilter) return false;
        return true;
    });

    if (filteredTasks.length === 0) {
        const p = document.createElement('p');
        p.textContent = 'No hay tareas que mostrar.';
        if (state.darkMode) p.style.color = 'var(--dark-text)';
        taskList.appendChild(p);
        return;
    }

    filteredTasks.forEach(task => {
        const taskItem = document.createElement('li');
        taskItem.className = `task-item priority-${task.priority}`;
        if (task.completed) {
            taskItem.classList.add('completed');
        }

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.className = 'completed-checkbox';
        checkbox.checked = task.completed;
        checkbox.addEventListener('change', () => toggleTaskComplete(task.id));

        const content = document.createElement('div');
        content.className = 'task-content';

        const title = document.createElement('h3');
        title.className = 'task-title';
        title.textContent = task.title;
        content.appendChild(title);

        if (task.description) {
            const description = document.createElement('p');
            description.className = 'task-description';
            description.textContent = task.description;
            content.appendChild(description);
        }

        const meta = document.createElement('div');
        meta.className = 'task-meta';

        const category = document.createElement('span');
        category.className = 'task-category';
        category.textContent = task.category.charAt(0).toUpperCase() + task.category.slice(1);
        meta.appendChild(category);

        const priority = document.createElement('span');
        priority.className = 'task-priority';
        priority.textContent = getPriorityText(task.priority);
        meta.appendChild(priority);

        if (task.dueDate) {
            const date = document.createElement('span');
            date.className = 'task-date';
            date.textContent = formatDateTime(task.dueDate);
            meta.appendChild(date);
        }

        content.appendChild(meta);

        const actions = document.createElement('div');
        actions.className = 'task-actions';

        const deleteBtn = document.createElement('button');
        deleteBtn.className = 'btn-icon btn-delete';
        deleteBtn.innerHTML = '🗑️';
        deleteBtn.title = 'Eliminar tarea';
        deleteBtn.addEventListener('click', () => deleteTask(task.id));
        actions.appendChild(deleteBtn);

        taskItem.appendChild(checkbox);
        taskItem.appendChild(content);
        taskItem.appendChild(actions);

        taskList.appendChild(taskItem);
    });
}

function updateStats() {
    document.getElementById('total-tasks').textContent = state.tasks.length;
    document.getElementById('completed-tasks').textContent = state.tasks.filter(task => task.completed).length;
    document.getElementById('pending-tasks').textContent = state.tasks.filter(task => !task.completed).length;
    document.getElementById('high-priority-tasks').textContent = state.tasks.filter(task => task.priority === 'high' && !task.completed).length;
}

function initCharts() {
    const weeklyCtx = document.getElementById('weekly-progress').getContext('2d');
    const monthlyCtx = document.getElementById('monthly-progress').getContext('2d');

    const initialTextColor = state.darkMode ? '#eee' : '#333';
    const initialGridColor = state.darkMode ? '#444' : '#ddd';

    // Define colors directly
    const completedColor = '#2ecc71'; // Verde
    const pendingColor = '#3498db';   // Azul


    const chartConfigBase = {
        type: 'bar',
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: { precision: 0, color: initialTextColor },
                    grid: { color: initialGridColor }
                },
                x: {
                    ticks: { color: initialTextColor },
                    grid: { color: initialGridColor }
                }
            },
            plugins: {
                legend: { position: 'top', labels: { color: initialTextColor } }
            }
        }
    };

    weeklyChart = new Chart(weeklyCtx, {
        ...chartConfigBase,
        data: {
            labels: getDaysOfWeek(),
            datasets: [
                { label: 'Completadas', data: [], backgroundColor: completedColor },
                { label: 'Pendientes', data: [], backgroundColor: pendingColor }
            ]
        }
    });

    monthlyChart = new Chart(monthlyCtx, {
        ...chartConfigBase,
        data: {
            labels: getDaysOfMonth(),
            datasets: [
                { label: 'Completadas', data: [], backgroundColor: completedColor },
                { label: 'Pendientes', data: [], backgroundColor: pendingColor }
            ]
        }
    });
}

function updateCharts() {
    if (!weeklyChart || !monthlyChart) return;

    const today = new Date();

    const weeklyData = getDaysOfWeek().map((dayName, index) => {
        const firstDayOfCurrentWeek = new Date(today);
        firstDayOfCurrentWeek.setDate(today.getDate() - today.getDay() + index);
        firstDayOfCurrentWeek.setHours(0, 0, 0, 0);

        const endOfDay = new Date(firstDayOfCurrentWeek);
        endOfDay.setHours(23, 59, 59, 999);

        const dayTasks = state.tasks.filter(task => {
            const taskCreatedAt = task.createdAt instanceof Date ? task.createdAt : new Date(task.createdAt);
            return taskCreatedAt >= firstDayOfCurrentWeek && taskCreatedAt <= endOfDay;
        });

        return {
            completed: dayTasks.filter(task => task.completed).length,
            pending: dayTasks.filter(task => !task.completed).length
        };
    });

    const monthlyData = getDaysOfMonth().map(dayNumber => {
        const currentMonth = today.getMonth();
        const currentYear = today.getFullYear();
        const day = parseInt(dayNumber);

        const startOfDay = new Date(currentYear, currentMonth, day, 0, 0, 0, 0);
        const endOfDay = new Date(currentYear, currentMonth, day, 23, 59, 59, 999);

        const dayTasks = state.tasks.filter(task => {
            const taskCreatedAt = task.createdAt instanceof Date ? task.createdAt : new Date(task.createdAt);
            return taskCreatedAt >= startOfDay && taskCreatedAt <= endOfDay;
        });

        return {
            completed: dayTasks.filter(task => task.completed).length,
            pending: dayTasks.filter(task => !task.completed).length
        };
    });

    weeklyChart.data.datasets[0].data = weeklyData.map(data => data.completed);
    weeklyChart.data.datasets[1].data = weeklyData.map(data => data.pending);

    monthlyChart.data.datasets[0].data = monthlyData.map(data => data.completed);
    monthlyChart.data.datasets[1].data = monthlyData.map(data => data.pending);

    weeklyChart.update();
    monthlyChart.update();
}

function toggleDarkMode() {
    state.darkMode = !state.darkMode;
    document.body.classList.toggle('dark-mode', state.darkMode);
    themeToggle.textContent = state.darkMode ? '☀️' : '🌙';
    localStorage.setItem('darkMode', state.darkMode.toString());
    document.dispatchEvent(new CustomEvent('themeChanged'));
}

function updateChartThemeColors() {
    if (!weeklyChart || !monthlyChart) return;

    const textColor = state.darkMode ? '#eee' : '#333';
    const gridColor = state.darkMode ? '#444' : '#ddd';

    const charts = [weeklyChart, monthlyChart];
    charts.forEach(chart => {
        if (chart && chart.options) {
            chart.options.scales.x.ticks.color = textColor;
            chart.options.scales.y.ticks.color = textColor;
            chart.options.scales.x.grid.color = gridColor;
            chart.options.scales.y.grid.color = gridColor;
            chart.options.plugins.legend.labels.color = textColor;
            chart.update('none');
        }
    });
}


// --- File Operations ---
function saveTasksToFile() {
    const tasksToSave = state.tasks.map(task => ({
        ...task,
        createdAt: task.createdAt instanceof Date ? task.createdAt.toISOString() : task.createdAt,
    }));
    const dataStr = JSON.stringify(tasksToSave, null, 2);
    const blob = new Blob([dataStr], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'tareas.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    alert('Tareas guardadas en tareas.json');
}

function loadTasksFromFile(file) {
    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            const loadedTasks = JSON.parse(e.target.result);
            if (Array.isArray(loadedTasks)) {
                state.tasks = loadedTasks.map(task => ({
                    ...task,
                    createdAt: task.createdAt ? new Date(task.createdAt) : null,
                }));
                saveTasksToLocalStorage();
                renderTasks();
                updateStats();
                updateCharts();
                alert('Tareas cargadas correctamente desde el archivo!');
            } else {
                alert('El archivo no tiene el formato esperado (debe ser un array de tareas).');
            }
        } catch (error) {
            console.error('Error al cargar tareas desde archivo:', error);
            alert('Error al leer el archivo de tareas. Verifique que sea un JSON válido.');
        }
    };
    reader.onerror = () => {
        alert('Error al leer el archivo.');
    };
    reader.readAsText(file);
}


// --- Service Worker Registration ---
if ('serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    // Usamos window.onload para registrar el SW después de que la página
    // y todos sus recursos principales se hayan cargado, para no ralentizar la carga inicial.
    navigator.serviceWorker.register('/service-worker.js') // Asegúrate de que la ruta sea correcta
      .then(registration => {
        console.log('Service Worker registrado con éxito. Scope:', registration.scope);
      })
      .catch(error => {
        console.log('Fallo en el registro del Service Worker:', error);
      });
  });
} else {
  console.log('Service Worker no soportado por este navegador.');
}