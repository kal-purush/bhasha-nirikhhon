// Import Firebase modules
import { auth, db } from './firebase-config.js';
import { onAuthStateChanged, signOut } from 'https://www.gstatic.com/firebasejs/9.22.0/firebase-auth.js';
import { collection, doc, getDoc, getDocs, setDoc, updateDoc, deleteDoc, query, where, orderBy, limit, serverTimestamp, addDoc } from 'https://www.gstatic.com/firebasejs/9.22.0/firebase-firestore.js';

// DOM Elements
const elements = {
    // Sidebar
    sidebarLinks: document.querySelectorAll('.sidebar-menu a'),
    logoutBtn: document.getElementById('logout-btn'),
    
    // Dashboard
    totalOrders: document.getElementById('total-orders'),
    totalRevenue: document.getElementById('total-revenue'),
    totalUsers: document.getElementById('total-users'),
    totalProducts: document.getElementById('total-products'),
    recentOrders: document.getElementById('recent-orders'),
    
    // Products
    productsTable: document.getElementById('products-table'),
    addProductBtn: document.getElementById('add-product-btn'),
    addProductModal: document.getElementById('add-product-modal'),
    addProductForm: document.getElementById('add-product-form'),
    saveProductBtn: document.getElementById('save-product-btn'),
    
    // Categories
    categoriesGrid: document.getElementById('categories-grid'),
    addCategoryBtn: document.getElementById('add-category-btn'),
    addCategoryModal: document.getElementById('add-category-modal'),
    addCategoryForm: document.getElementById('add-category-form'),
    saveCategoryBtn: document.getElementById('save-category-btn'),
    
    // Orders
    ordersTable: document.getElementById('orders-table'),
    orderStatusFilter: document.getElementById('order-status-filter'),
    
    // Users
    usersTable: document.getElementById('users-table'),
    
    // Settings
    generalSettingsForm: document.getElementById('general-settings-form'),
    paymentSettingsForm: document.getElementById('payment-settings-form'),
    
    // Modals
    modals: document.querySelectorAll('.modal'),
    closeModalBtns: document.querySelectorAll('.close-modal'),
    
    // Page Title
    pageTitle: document.getElementById('page-title'),

    variantsContainer: document.getElementById('variants-container'),
    addVariantGroupBtn: document.getElementById('add-variant-group'),
    variantPricesContainer: document.getElementById('variant-prices-container'),
};

// State Management
let currentUser = null;
let currentTab = 'dashboard';

// Initialize the admin panel
async function initializeAdminPanel() {
    try {
        // Check authentication state
        onAuthStateChanged(auth, async (user) => {
            if (user) {
                currentUser = user;
                await loadUserProfile();
                await loadDashboardData();
                setupEventListeners();
            } else {
                window.location.href = 'index.html';
            }
        });
    } catch (error) {
        showNotification('Error initializing admin panel: ' + error.message, 'error');
    }
}

// Load user profile
async function loadUserProfile() {
    try {
        const userDoc = await getDoc(doc(db, 'users', currentUser.uid));
        if (userDoc.exists()) {
            const userData = userDoc.data();
            document.getElementById('admin-name').textContent = userData.name || 'Admin';
        }
    } catch (error) {
        showNotification('Error loading user profile: ' + error.message, 'error');
    }
}

// Load dashboard data
async function loadDashboardData() {
    try {
        // Load total orders
        const ordersQuery = query(collection(db, 'orders'));
        const ordersSnapshot = await getDocs(ordersQuery);
        const totalOrders = ordersSnapshot.size;
        elements.totalOrders.textContent = totalOrders;

        // Calculate total revenue
        let totalRevenue = 0;
        ordersSnapshot.forEach(doc => {
            const orderData = doc.data();
            totalRevenue += orderData.total || 0;
        });
        elements.totalRevenue.textContent = `$${totalRevenue.toFixed(2)}`;

        // Load total users
        const usersQuery = query(collection(db, 'users'));
        const usersSnapshot = await getDocs(usersQuery);
        elements.totalUsers.textContent = usersSnapshot.size;

        // Load total products
        const productsQuery = query(collection(db, 'products'));
        const productsSnapshot = await getDocs(productsQuery);
        elements.totalProducts.textContent = productsSnapshot.size;

        // Load recent orders
        const recentOrdersQuery = query(
            collection(db, 'orders'),
            orderBy('createdAt', 'desc'),
            limit(5)
        );
        const recentOrdersSnapshot = await getDocs(recentOrdersQuery);
        elements.recentOrders.innerHTML = '';
        
        recentOrdersSnapshot.forEach(doc => {
            const orderData = doc.data();
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${doc.id}</td>
                <td>${orderData.customerName || 'N/A'}</td>
                <td>${new Date(orderData.createdAt?.toDate()).toLocaleDateString()}</td>
                <td>$${orderData.total?.toFixed(2) || '0.00'}</td>
                <td><span class="status-badge status-${orderData.status || 'pending'}">${orderData.status || 'Pending'}</span></td>
                <td>
                    <div class="action-buttons">
                        <button class="action-btn action-btn-edit" onclick="viewOrderDetails('${doc.id}')">
                            <i class="fas fa-eye"></i>
                        </button>
                    </div>
                </td>
            `;
            elements.recentOrders.appendChild(row);
        });
    } catch (error) {
        showNotification('Error loading dashboard data: ' + error.message, 'error');
    }
}

// Load products
async function loadProducts() {
    try {
        const productsTable = document.getElementById('products-table');
        if (!productsTable) {
            console.error('Products table element not found');
            return;
        }
        
        productsTable.innerHTML = '';
        
        const productsSnapshot = await getDocs(collection(db, 'products'));
        if (productsSnapshot.empty) {
            productsTable.innerHTML = '<tr><td colspan="7" class="text-center">No products found</td></tr>';
            return;
        }
        
        productsSnapshot.forEach(doc => {
            const product = doc.data();
            const row = document.createElement('tr');
            
            // Create image cell with error handling
            const imageCell = document.createElement('td');
            const img = document.createElement('img');
            img.src = product.image;
            img.alt = product.name;
            img.style.width = '50px';
            img.style.height = '50px';
            img.style.objectFit = 'cover';
            img.style.borderRadius = '4px';
            
            // Add error handling for image loading
            img.onerror = function() {
                this.src = 'https://via.placeholder.com/50?text=No+Image';
                this.alt = 'Image not available';
            };
            
            imageCell.appendChild(img);
            
            row.innerHTML = `
                <td>${product.name}</td>
                <td>${product.category}</td>
                <td>$${product.price.toFixed(2)}</td>
                <td>${product.variants ? product.variants.length : 0} variants</td>
                <td>${product.customFields ? product.customFields.length : 0} custom fields</td>
                <td>
                    <div class="action-buttons">
                        <button class="action-btn action-btn-edit" data-id="${doc.id}">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="action-btn action-btn-delete" data-id="${doc.id}">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </td>
            `;
            
            // Insert image cell at the beginning
            row.insertBefore(imageCell, row.firstChild);
            
            productsTable.appendChild(row);
        });
        
        // Update total products count
        const totalProductsElement = document.getElementById('total-products');
        if (totalProductsElement) {
            totalProductsElement.textContent = productsSnapshot.size;
        }
        
        // Add event listeners for edit and delete buttons
        document.querySelectorAll('.action-btn-edit').forEach(button => {
            button.addEventListener('click', () => editProduct(button.dataset.id));
        });
        
        document.querySelectorAll('.action-btn-delete').forEach(button => {
            button.addEventListener('click', () => deleteProduct(button.dataset.id));
        });
    } catch (error) {
        console.error('Error loading products:', error);
        showNotification('Error loading products', 'error');
    }
}

// Load categories
async function loadCategories() {
    try {
        // First, ensure default categories exist
        await initializeDefaultCategories();
        
        const categoriesQuery = query(collection(db, 'categories'));
        const categoriesSnapshot = await getDocs(categoriesQuery);
        elements.categoriesGrid.innerHTML = '';
        
        categoriesSnapshot.forEach(doc => {
            const categoryData = doc.data();
            const card = document.createElement('div');
            card.className = 'category-card';
            card.innerHTML = `
                <div class="category-icon">
                    <i class="${categoryData.icon}"></i>
                </div>
                <h3>${categoryData.name}</h3>
                <p>${categoryData.description || ''}</p>
                <div class="category-actions">
                    <button class="btn btn-primary" onclick="editCategory('${doc.id}')">
                        <i class="fas fa-edit"></i> Edit
                    </button>
                    <button class="btn btn-danger" onclick="deleteCategory('${doc.id}')">
                        <i class="fas fa-trash"></i> Delete
                    </button>
                </div>
            `;
            elements.categoriesGrid.appendChild(card);
        });
    } catch (error) {
        showNotification('Error loading categories: ' + error.message, 'error');
    }
}

// Initialize default categories
async function initializeDefaultCategories() {
    try {
        const defaultCategories = [
            {
                name: 'Mobile Games',
                icon: 'fas fa-mobile-alt',
                description: 'Top-up cards for popular mobile games',
                slug: 'mobile-games'
            },
            {
                name: 'PC Games',
                icon: 'fas fa-desktop',
                description: 'Digital codes for PC gaming platforms',
                slug: 'pc-games'
            },
            {
                name: 'Console Games',
                icon: 'fas fa-tv',
                description: 'Digital codes for console gaming platforms',
                slug: 'console-games'
            },
            {
                name: 'Gift Cards',
                icon: 'fas fa-gift',
                description: 'Digital gift cards for various services',
                slug: 'gift-cards'
            }
        ];
        
        // Check if categories already exist
        const categoriesQuery = query(collection(db, 'categories'));
        const categoriesSnapshot = await getDocs(categoriesQuery);
        
        // If no categories exist, add the default ones
        if (categoriesSnapshot.empty) {
            for (const category of defaultCategories) {
                await addDoc(collection(db, 'categories'), {
                    ...category,
                    createdAt: serverTimestamp(),
                    updatedAt: serverTimestamp()
                });
            }
            showNotification('Default categories initialized', 'success');
        }
    } catch (error) {
        console.error('Error initializing default categories:', error);
        showNotification('Error initializing default categories: ' + error.message, 'error');
    }
}

// Load orders
async function loadOrders() {
    try {
        const statusFilter = elements.orderStatusFilter.value;
        let ordersQuery = query(collection(db, 'orders'), orderBy('createdAt', 'desc'));
        
        if (statusFilter !== 'all') {
            ordersQuery = query(ordersQuery, where('status', '==', statusFilter));
        }
        
        const ordersSnapshot = await getDocs(ordersQuery);
        elements.ordersTable.innerHTML = '';
        
        ordersSnapshot.forEach(doc => {
            const orderData = doc.data();
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${doc.id}</td>
                <td>${orderData.customerName || 'N/A'}</td>
                <td>${new Date(orderData.createdAt?.toDate()).toLocaleDateString()}</td>
                <td>${orderData.items?.length || 0} items</td>
                <td>$${orderData.total?.toFixed(2) || '0.00'}</td>
                <td><span class="status-badge status-${orderData.status || 'pending'}">${orderData.status || 'Pending'}</span></td>
                <td>
                    <div class="action-buttons">
                        <button class="action-btn action-btn-edit" onclick="viewOrderDetails('${doc.id}')">
                            <i class="fas fa-eye"></i>
                        </button>
                        <button class="action-btn action-btn-delete" onclick="deleteOrder('${doc.id}')">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </td>
            `;
            elements.ordersTable.appendChild(row);
        });
    } catch (error) {
        showNotification('Error loading orders: ' + error.message, 'error');
    }
}

// Load users
async function loadUsers() {
    try {
        const usersQuery = query(collection(db, 'users'));
        const usersSnapshot = await getDocs(usersQuery);
        elements.usersTable.innerHTML = '';
        
        usersSnapshot.forEach(doc => {
            const userData = doc.data();
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${doc.id}</td>
                <td>${userData.name || 'N/A'}</td>
                <td>${userData.email || 'N/A'}</td>
                <td>${new Date(userData.createdAt?.toDate()).toLocaleDateString()}</td>
                <td>${userData.totalOrders || 0}</td>
                <td>
                    <div class="action-buttons">
                        <button class="action-btn action-btn-edit" onclick="viewUserDetails('${doc.id}')">
                            <i class="fas fa-eye"></i>
                        </button>
                    </div>
                </td>
            `;
            elements.usersTable.appendChild(row);
        });
    } catch (error) {
        showNotification('Error loading users: ' + error.message, 'error');
    }
}

// Setup event listeners
function setupEventListeners() {
    // Sidebar navigation
    elements.sidebarLinks.forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const tab = link.getAttribute('data-tab');
            if (tab) {
                switchTab(tab);
            }
        });
    });

    // Logout
    elements.logoutBtn.addEventListener('click', async () => {
        try {
            await signOut(auth);
            window.location.href = 'index.html';
        } catch (error) {
            showNotification('Error signing out: ' + error.message, 'error');
        }
    });

    // Add Product
    elements.addProductBtn.addEventListener('click', async () => {
        await loadCategoriesIntoDropdowns();
        showModal(elements.addProductModal);
        
        // Initialize product form after the modal is shown
        setTimeout(() => {
            initializeProductForm();
        }, 100);
    });

    // Add Variant Group
    if (elements.addVariantGroupBtn) {
        elements.addVariantGroupBtn.addEventListener('click', () => {
            const variantGroup = document.createElement('div');
            variantGroup.className = 'variant-group';
            variantGroup.innerHTML = `
                <div class="form-group">
                    <input type="text" class="variant-name" placeholder="Variant Name" required>
                </div>
                <div class="form-group">
                    <input type="number" class="variant-price" placeholder="Variant Price" step="0.01" required>
                </div>
                <button type="button" class="btn btn-danger remove-variant">Remove</button>
            `;
            elements.variantsContainer.appendChild(variantGroup);
            
            // Add event listener to remove button
            const removeBtn = variantGroup.querySelector('.remove-variant');
            removeBtn.addEventListener('click', () => {
                variantGroup.remove();
            });
        });
    }

    // Initialize existing variant groups
    document.querySelectorAll('.variant-group').forEach(group => {
        initializeVariantGroupButtons(group);
    });

    // Add Custom Field button in edit modal
    const editAddFieldBtn = document.getElementById('edit-add-field-btn');
    if (editAddFieldBtn) {
        editAddFieldBtn.addEventListener('click', () => {
            const customFieldsContainer = document.getElementById('edit-custom-fields-container');
            const fieldGroup = document.createElement('div');
            fieldGroup.className = 'custom-field-group';
            fieldGroup.innerHTML = `
                <div class="form-group">
                    <input type="text" class="custom-field-label" placeholder="Field Label" required>
                    <select class="custom-field-type">
                        <option value="text">Text</option>
                        <option value="select">Select</option>
                        <option value="checkbox">Checkbox</option>
                    </select>
                    <div class="custom-field-options" style="display: none">
                        <input type="text" class="custom-field-options-input" placeholder="Options (comma-separated)">
                    </div>
                    <button type="button" class="btn btn-danger remove-custom-field">Remove</button>
                </div>
            `;
            customFieldsContainer.appendChild(fieldGroup);
            initializeCustomFieldGroup(fieldGroup);
        });
    }

    // Save Product
    elements.saveProductBtn.addEventListener('click', async () => {
        try {
            // Get form values directly from input elements instead of FormData
            const name = document.getElementById('product-name').value;
            const category = document.getElementById('product-category').value;
            const price = parseFloat(document.getElementById('product-price').value);
            const description = document.getElementById('product-description').value;
            const image = document.getElementById('product-image').value;
            const status = document.getElementById('product-status').value;
            const variants = getVariantsData();
            
            // Validate required fields
            if (!name || !category || isNaN(price) || !image) {
                showNotification('Please fill in all required fields', 'error');
                return;
            }
            
            const productData = {
                name: name,
                category: category,
                price: price,
                description: description,
                image: image,
                status: status,
                variants: variants,
                createdAt: serverTimestamp()
            };

            // Create a new document with auto-generated ID
            const productsRef = collection(db, 'products');
            await addDoc(productsRef, productData);
            
            showNotification('Product added successfully', 'success');
            hideModal(elements.addProductModal);
            loadProducts();
        } catch (error) {
            console.error('Error adding product:', error);
            showNotification('Error adding product: ' + error.message, 'error');
        }
    });

    // Add Category
    elements.addCategoryBtn.addEventListener('click', () => {
        showModal(elements.addCategoryModal);
    });

    // Save Category
    elements.saveCategoryBtn.addEventListener('click', async () => {
        try {
            const formData = new FormData(elements.addCategoryForm);
            const categoryData = {
                name: formData.get('category-name'),
                icon: formData.get('category-icon'),
                description: formData.get('category-description'),
                slug: formData.get('category-name').toLowerCase().replace(/\s+/g, '-'),
                createdAt: serverTimestamp(),
                updatedAt: serverTimestamp()
            };

            await addDoc(collection(db, 'categories'), categoryData);
            showNotification('Category added successfully', 'success');
            hideModal(elements.addCategoryModal);
            loadCategories();
        } catch (error) {
            showNotification('Error adding category: ' + error.message, 'error');
        }
    });

    // Order status filter
    elements.orderStatusFilter.addEventListener('change', loadOrders);

    // Close modals
    elements.closeModalBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            const modal = btn.closest('.modal');
            hideModal(modal);
        });
    });

    // Update Category
    document.getElementById('update-category-btn').addEventListener('click', async () => {
        try {
            const categoryId = document.getElementById('edit-category-id').value;
            const categoryData = {
                name: document.getElementById('edit-category-name').value,
                icon: document.getElementById('edit-category-icon').value,
                description: document.getElementById('edit-category-description').value,
                slug: document.getElementById('edit-category-name').value.toLowerCase().replace(/\s+/g, '-'),
                updatedAt: serverTimestamp()
            };

            await updateDoc(doc(db, 'categories', categoryId), categoryData);
            showNotification('Category updated successfully', 'success');
            document.getElementById('edit-category-modal').style.display = 'none';
            loadCategories();
        } catch (error) {
            showNotification('Error updating category: ' + error.message, 'error');
        }
    });

    // Settings forms
    elements.generalSettingsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        try {
            const formData = new FormData(e.target);
            // Save general settings
            showNotification('Settings saved successfully', 'success');
        } catch (error) {
            showNotification('Error saving settings: ' + error.message, 'error');
        }
    });

    elements.paymentSettingsForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        try {
            const formData = new FormData(e.target);
            // Save payment settings
            showNotification('Settings saved successfully', 'success');
        } catch (error) {
            showNotification('Error saving settings: ' + error.message, 'error');
        }
    });

    // Update product
    document.getElementById('update-product-btn').addEventListener('click', async () => {
        try {
            const form = document.getElementById('edit-product-form');
            const productId = form.querySelector('#edit-product-id').value;
            
            // Get form values
            const name = form.querySelector('#edit-product-name').value;
            const category = form.querySelector('#edit-product-category').value;
            const price = parseFloat(form.querySelector('#edit-product-price').value);
            const description = form.querySelector('#edit-product-description').value;
            const image = form.querySelector('#edit-product-image').value;
            
            // Get variants data
            const variants = [];
            const variantGroups = form.querySelectorAll('.variant-group');
            variantGroups.forEach(group => {
                const nameInput = group.querySelector('.variant-name');
                const priceInput = group.querySelector('.variant-price');
                if (nameInput.value && priceInput.value) {
                    variants.push({
                        name: nameInput.value,
                        price: parseFloat(priceInput.value)
                    });
                }
            });

            // Get custom fields data
            const customFields = getCustomFieldsData();
            
            // Validate required fields
            if (!name || !category || isNaN(price) || !image) {
                showNotification('Please fill in all required fields', 'error');
                return;
            }
            
            // Update product in Firestore
            const productRef = doc(db, 'products', productId);
            await updateDoc(productRef, {
                name,
                category,
                price,
                description,
                image,
                variants,
                customFields,
                updatedAt: serverTimestamp()
            });
            
            showNotification('Product updated successfully', 'success');
            hideModal(document.getElementById('edit-product-modal'));
            loadProducts(); // Reload the products table
        } catch (error) {
            showNotification('Error updating product: ' + error.message, 'error');
        }
    });
}

// Switch tabs
function switchTab(tab) {
    // Update active tab
    elements.sidebarLinks.forEach(link => {
        link.classList.remove('active');
        if (link.getAttribute('data-tab') === tab) {
            link.classList.add('active');
        }
    });

    // Update page title
    elements.pageTitle.textContent = tab.charAt(0).toUpperCase() + tab.slice(1);

    // Hide all tab contents
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });

    // Show selected tab content
    const selectedTab = document.getElementById(`${tab}-tab`);
    if (selectedTab) {
        selectedTab.classList.add('active');
    }

    // Load tab data
    switch (tab) {
        case 'dashboard':
            loadDashboardData();
            break;
        case 'products':
            loadProducts();
            break;
        case 'categories':
            loadCategories();
            break;
        case 'orders':
            loadOrders();
            break;
        case 'users':
            loadUsers();
            break;
    }

    currentTab = tab;
}

// Modal functions
function showModal(modal) {
    if (modal) {
        modal.style.display = 'block';
    }
}

function hideModal(modal) {
    if (modal) {
        modal.style.display = 'none';
    }
}

// Notification system
function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.innerHTML = `
        <i class="fas ${getNotificationIcon(type)}"></i>
        <span>${message}</span>
    `;
    document.body.appendChild(notification);

    // Remove notification after 3 seconds
    setTimeout(() => {
        notification.remove();
    }, 3000);
}

function getNotificationIcon(type) {
    switch (type) {
        case 'success':
            return 'fa-check-circle';
        case 'error':
            return 'fa-exclamation-circle';
        case 'warning':
            return 'fa-exclamation-triangle';
        case 'info':
        default:
            return 'fa-info-circle';
    }
}

// Global functions for product management
window.editProduct = async (productId) => {
    try {
        const productDoc = await getDoc(doc(db, 'products', productId));
        if (productDoc.exists()) {
            const productData = productDoc.data();
            const editModal = document.getElementById('edit-product-modal');
            const form = document.getElementById('edit-product-form');

            await loadCategoriesIntoDropdowns();

            // Populate form fields
            form.querySelector('#edit-product-id').value = productId;
            form.querySelector('#edit-product-name').value = productData.name;
            form.querySelector('#edit-product-category').value = productData.category;
            form.querySelector('#edit-product-price').value = productData.price;
            form.querySelector('#edit-product-description').value = productData.description;
            form.querySelector('#edit-product-image').value = productData.image;
            
            // Handle variants if they exist
            const variantsContainer = form.querySelector('#variants-container');
            if (variantsContainer) {
                variantsContainer.innerHTML = ''; // Clear existing variants
                if (productData.variants && productData.variants.length > 0) {
                    productData.variants.forEach(variant => {
                        const variantGroup = document.createElement('div');
                        variantGroup.className = 'variant-group';
                        variantGroup.innerHTML = `
                            <input type="text" class="variant-name" placeholder="Variant Name" value="${variant.name}">
                            <input type="number" class="variant-price" placeholder="Price in Rs" step="0.01" value="${variant.price}">
                            <button type="button" class="btn btn-danger remove-variant">Remove</button>
                        `;
                        variantsContainer.appendChild(variantGroup);
                        initializeVariantGroupButtons(variantGroup);
                    });
                } else {
                    // Add one empty variant group if no variants exist
                    const variantGroup = document.createElement('div');
                    variantGroup.className = 'variant-group';
                    variantGroup.innerHTML = `
                        <input type="text" class="variant-name" placeholder="Variant Name">
                        <input type="number" class="variant-price" placeholder="Price in Rs" step="0.01">
                        <button type="button" class="btn btn-danger remove-variant">Remove</button>
                    `;
                    variantsContainer.appendChild(variantGroup);
                    initializeVariantGroupButtons(variantGroup);
                }
            }

            // Handle custom fields
            const customFieldsContainer = form.querySelector('#edit-custom-fields-container');
            if (customFieldsContainer) {
                customFieldsContainer.innerHTML = ''; // Clear existing custom fields
                if (productData.customFields && productData.customFields.length > 0) {
                    productData.customFields.forEach(field => {
                        const fieldGroup = document.createElement('div');
                        fieldGroup.className = 'custom-field-group';
                        fieldGroup.innerHTML = `
                            <div class="form-group">
                                <input type="text" class="custom-field-label" placeholder="Field Label" value="${field.label}">
                                <select class="custom-field-type">
                                    <option value="text" ${field.type === 'text' ? 'selected' : ''}>Text</option>
                                    <option value="select" ${field.type === 'select' ? 'selected' : ''}>Select</option>
                                    <option value="checkbox" ${field.type === 'checkbox' ? 'selected' : ''}>Checkbox</option>
                                </select>
                                <div class="custom-field-options" style="display: ${field.type === 'select' ? 'block' : 'none'}">
                                    <input type="text" class="custom-field-options-input" placeholder="Options (comma-separated)" value="${field.options ? field.options.join(', ') : ''}">
                                </div>
                                <button type="button" class="btn btn-danger remove-custom-field">Remove</button>
                            </div>
                        `;
                        customFieldsContainer.appendChild(fieldGroup);
                        initializeCustomFieldGroup(fieldGroup);
                    });
                }
            }

            // Initialize add custom field button
            const addFieldBtn = form.querySelector('#edit-add-field-btn');
            if (addFieldBtn) {
                addFieldBtn.addEventListener('click', () => addCustomField('text', 'edit-custom-fields-container'));
            }

            showModal(editModal);
        }
    } catch (error) {
        showNotification('Error loading product: ' + error.message, 'error');
    }
};

window.deleteProduct = async (productId) => {
    if (confirm('Are you sure you want to delete this product?')) {
        try {
            await deleteDoc(doc(db, 'products', productId));
            showNotification('Product deleted successfully', 'success');
            loadProducts();
        } catch (error) {
            showNotification('Error deleting product: ' + error.message, 'error');
        }
    }
};

// Edit category
window.editCategory = async function(categoryId) {
    try {
        const categoryDoc = await getDoc(doc(db, 'categories', categoryId));
        if (!categoryDoc.exists()) {
            showNotification('Category not found', 'error');
            return;
        }
        
        const categoryData = categoryDoc.data();
        
        // Populate the edit category modal
        document.getElementById('edit-category-id').value = categoryId;
        document.getElementById('edit-category-name').value = categoryData.name;
        document.getElementById('edit-category-icon').value = categoryData.icon;
        document.getElementById('edit-category-description').value = categoryData.description || '';
        
        // Show the modal
        document.getElementById('edit-category-modal').style.display = 'block';
    } catch (error) {
        showNotification('Error loading category: ' + error.message, 'error');
    }
};

// Delete category
window.deleteCategory = async function(categoryId) {
    try {
        if (confirm('Are you sure you want to delete this category? This action cannot be undone.')) {
            await deleteDoc(doc(db, 'categories', categoryId));
            showNotification('Category deleted successfully', 'success');
            loadCategories(); // Reload the categories
        }
    } catch (error) {
        showNotification('Error deleting category: ' + error.message, 'error');
    }
};

// Global function for viewing user details
window.viewUserDetails = async (userId) => {
    try {
        const userDoc = await getDoc(doc(db, 'users', userId));
        if (userDoc.exists()) {
            const userData = userDoc.data();
            const modal = document.getElementById('user-details-modal');

            // Populate user details
            document.getElementById('user-id').value = userId;
            document.getElementById('user-name').value = userData.name || 'N/A';
            document.getElementById('user-email').value = userData.email || 'N/A';
            document.getElementById('user-joined-date').value = new Date(userData.createdAt?.toDate()).toLocaleDateString();
            document.getElementById('user-total-orders').value = userData.totalOrders || 0;
            document.getElementById('user-total-spent').value = `$${userData.totalSpent?.toFixed(2) || '0.00'}`;

            showModal(modal);
        }
    } catch (error) {
        showNotification('Error loading user details: ' + error.message, 'error');
    }
};

// Global function for viewing order details
window.viewOrderDetails = async (orderId) => {
    try {
        const orderDoc = await getDoc(doc(db, 'orders', orderId));
        if (orderDoc.exists()) {
            const orderData = orderDoc.data();
            const modal = document.getElementById('order-details-modal');

            // Populate order details
            document.getElementById('order-id').textContent = orderId;
            document.getElementById('order-customer').textContent = orderData.customerName || 'N/A';
            document.getElementById('order-date').textContent = new Date(orderData.createdAt?.toDate()).toLocaleDateString();
            document.getElementById('order-status').value = orderData.status || 'pending';

            // Populate order items
            const itemsTable = document.getElementById('order-items-table');
            itemsTable.innerHTML = '';
            orderData.items?.forEach(item => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${item.name}</td>
                    <td>$${item.price?.toFixed(2) || '0.00'}</td>
                    <td>${item.quantity || 1}</td>
                    <td>$${(item.price * (item.quantity || 1)).toFixed(2)}</td>
                `;
                itemsTable.appendChild(row);
            });

            // Populate totals
            document.getElementById('order-subtotal').textContent = `$${orderData.subtotal?.toFixed(2) || '0.00'}`;
            document.getElementById('order-shipping').textContent = `$${orderData.serviceFee?.toFixed(2) || '0.00'}`;
            document.getElementById('order-total').textContent = `$${orderData.total?.toFixed(2) || '0.00'}`;

            showModal(modal);
        }
    } catch (error) {
        showNotification('Error loading order details: ' + error.message, 'error');
    }
};

// Global function for deleting orders
window.deleteOrder = async (orderId) => {
    if (confirm('Are you sure you want to delete this order?')) {
        try {
            await deleteDoc(doc(db, 'orders', orderId));
            showNotification('Order deleted successfully', 'success');
            loadOrders();
        } catch (error) {
            showNotification('Error deleting order: ' + error.message, 'error');
        }
    }
};

// Load categories into product form dropdowns
async function loadCategoriesIntoDropdowns() {
    try {
        const categoriesQuery = query(collection(db, 'categories'));
        const categoriesSnapshot = await getDocs(categoriesQuery);
        const categorySelects = document.querySelectorAll('#product-category, #edit-product-category');
        
        categorySelects.forEach(select => {
            select.innerHTML = '<option value="">Select a category</option>';
            categoriesSnapshot.forEach(doc => {
                const categoryData = doc.data();
                const option = document.createElement('option');
                option.value = categoryData.name;
                option.textContent = categoryData.name;
                select.appendChild(option);
            });
        });
    } catch (error) {
        showNotification('Error loading categories into dropdowns: ' + error.message, 'error');
    }
}

// Initialize variant functionality
function initializeVariantFunctionality() {
    const addVariantGroupBtn = document.getElementById('add-variant-group');
    if (addVariantGroupBtn) {
        addVariantGroupBtn.addEventListener('click', () => {
            const variantsContainer = document.getElementById('variants-container');
            const variantGroup = document.createElement('div');
            variantGroup.className = 'variant-group';
            variantGroup.innerHTML = `
                <div class="form-group">
                    <input type="text" class="variant-name" placeholder="Variant Name" required>
                </div>
                <div class="form-group">
                    <input type="number" class="variant-price" placeholder="Variant Price" step="0.01" required>
                </div>
                <button type="button" class="btn btn-danger remove-variant">Remove</button>
            `;
            variantsContainer.appendChild(variantGroup);
            
            // Add event listener to remove button
            const removeBtn = variantGroup.querySelector('.remove-variant');
            removeBtn.addEventListener('click', () => {
                variantGroup.remove();
            });
        });
    }
}

// Initialize buttons for a variant group
function initializeVariantGroupButtons(variantGroup) {
    // Remove variant button
    const removeBtn = variantGroup.querySelector('.remove-variant');
    removeBtn.addEventListener('click', () => {
        variantGroup.remove();
    });
}

// Initialize custom field group functionality
function initializeCustomFieldGroup(fieldGroup) {
    const typeSelect = fieldGroup.querySelector('.custom-field-type');
    const optionsDiv = fieldGroup.querySelector('.custom-field-options');
    const removeBtn = fieldGroup.querySelector('.remove-custom-field');
    
    // Handle type change
    typeSelect.addEventListener('change', () => {
        optionsDiv.style.display = typeSelect.value === 'select' ? 'block' : 'none';
    });
    
    // Handle remove button
    removeBtn.addEventListener('click', () => {
        fieldGroup.remove();
    });
}

// Get variants data from form
function getVariantsData() {
    const variants = [];
    const variantGroups = document.querySelectorAll('.variant-group');
    
    if (variantGroups.length === 0) {
        return variants;
    }
    
    variantGroups.forEach(group => {
        const nameInput = group.querySelector('.variant-name');
        const priceInput = group.querySelector('.variant-price');
        
        if (nameInput && priceInput) {
            const name = nameInput.value.trim();
            const price = parseFloat(priceInput.value);
            
            // Only add variants that have both name and price
            if (name && !isNaN(price) && price > 0) {
                variants.push({ 
                    name: name, 
                    price: price 
                });
            }
        }
    });
    
    return variants;
}

// Add custom field functionality
function addCustomField(type) {
    const customFieldsContainer = document.getElementById('custom-fields-container');
    const fieldId = 'custom-field-' + Date.now();
    
    const fieldWrapper = document.createElement('div');
    fieldWrapper.className = 'custom-field-wrapper';
    fieldWrapper.innerHTML = `
        <div class="custom-field">
            <input type="text" class="field-label" placeholder="Field Label" required>
            <select class="field-type">
                <option value="text" ${type === 'text' ? 'selected' : ''}>Text Input</option>
                <option value="number" ${type === 'number' ? 'selected' : ''}>Number Input</option>
                <option value="email" ${type === 'email' ? 'selected' : ''}>Email Input</option>
                <option value="tel" ${type === 'tel' ? 'selected' : ''}>Telephone Input</option>
                <option value="date" ${type === 'date' ? 'selected' : ''}>Date Input</option>
                <option value="textarea" ${type === 'textarea' ? 'selected' : ''}>Text Area</option>
                <option value="select" ${type === 'select' ? 'selected' : ''}>Select Option</option>
                <option value="radio" ${type === 'radio' ? 'selected' : ''}>Radio Buttons</option>
                <option value="checkbox" ${type === 'checkbox' ? 'selected' : ''}>Checkbox</option>
                <option value="checkbox-group" ${type === 'checkbox-group' ? 'selected' : ''}>Checkbox Group</option>
            </select>
            <div class="field-options" style="display: ${['select', 'radio', 'checkbox-group'].includes(type) ? 'block' : 'none'}">
                <input type="text" class="field-option" placeholder="Option 1">
                <input type="text" class="field-option" placeholder="Option 2">
                <button type="button" class="add-option-btn">+ Add Option</button>
            </div>
            <button type="button" class="remove-field-btn">Remove</button>
        </div>
    `;
    
    customFieldsContainer.appendChild(fieldWrapper);
    
    // Add event listeners
    const fieldType = fieldWrapper.querySelector('.field-type');
    const fieldOptions = fieldWrapper.querySelector('.field-options');
    const addOptionBtn = fieldWrapper.querySelector('.add-option-btn');
    const removeFieldBtn = fieldWrapper.querySelector('.remove-field-btn');
    
    fieldType.addEventListener('change', () => {
        fieldOptions.style.display = ['select', 'radio', 'checkbox-group'].includes(fieldType.value) ? 'block' : 'none';
    });
    
    addOptionBtn.addEventListener('click', () => {
        const optionInput = document.createElement('input');
        optionInput.type = 'text';
        optionInput.className = 'field-option';
        optionInput.placeholder = `Option ${fieldOptions.querySelectorAll('.field-option').length + 1}`;
        fieldOptions.insertBefore(optionInput, addOptionBtn);
    });
    
    removeFieldBtn.addEventListener('click', () => {
        fieldWrapper.remove();
    });
}

// Get custom fields data
function getCustomFieldsData() {
    const customFields = [];
    const customFieldWrappers = document.querySelectorAll('.custom-field-wrapper');
    
    customFieldWrappers.forEach(wrapper => {
        const label = wrapper.querySelector('.field-label').value;
        const type = wrapper.querySelector('.field-type').value;
        
        if (label && type) {
            const field = {
                label,
                type,
                required: false
            };
            
            // Add options for select, radio, and checkbox-group types
            if (['select', 'radio', 'checkbox-group'].includes(type)) {
                const optionInputs = wrapper.querySelectorAll('.field-option');
                const options = [];
                
                optionInputs.forEach(input => {
                    if (input.value.trim()) {
                        options.push(input.value.trim());
                    }
                });
                
                if (options.length > 0) {
                    field.options = options;
                }
            }
            
            customFields.push(field);
        }
    });
    
    return customFields;
}

// Save product with custom fields
async function saveProduct() {
    try {
        console.log('Starting saveProduct function');
        
        const form = document.getElementById('add-product-form');
        if (!form) {
            throw new Error('Product form not found');
        }
        
        // Get form values directly from input elements
        const nameInput = document.getElementById('product-name');
        const descriptionInput = document.getElementById('product-description');
        const priceInput = document.getElementById('product-price');
        const categoryInput = document.getElementById('product-category');
        const imageInput = document.getElementById('image');
        
        // Log the state of each input element
        console.log('Form elements:', {
            nameInput: nameInput?.value,
            descriptionInput: descriptionInput?.value,
            priceInput: priceInput?.value,
            categoryInput: categoryInput?.value,
            imageInput: imageInput?.value
        });
        
        // Check if all required inputs exist
        if (!nameInput || !descriptionInput || !priceInput || !categoryInput || !imageInput) {
            const missingFields = [];
            if (!nameInput) missingFields.push('name');
            if (!descriptionInput) missingFields.push('description');
            if (!priceInput) missingFields.push('price');
            if (!categoryInput) missingFields.push('category');
            if (!imageInput) missingFields.push('image');
            throw new Error(`Missing required fields: ${missingFields.join(', ')}`);
        }
        
        // Validate input values
        if (!nameInput.value.trim()) throw new Error('Product name is required');
        if (!descriptionInput.value.trim()) throw new Error('Product description is required');
        if (!priceInput.value || isNaN(parseFloat(priceInput.value))) throw new Error('Valid price is required');
        if (!categoryInput.value) throw new Error('Category is required');
        if (!imageInput.value.trim()) throw new Error('Image URL is required');
        
        // Validate image URL
        const imageUrl = imageInput.value.trim();
        try {
            const response = await fetch(imageUrl, { method: 'HEAD' });
            if (!response.ok) {
                throw new Error('Invalid image URL');
            }
        } catch (error) {
            throw new Error('Invalid image URL or image not accessible');
        }
        
        const name = nameInput.value.trim();
        const description = descriptionInput.value.trim();
        const price = parseFloat(priceInput.value);
        const category = categoryInput.value;
        const image = imageUrl;
        
        // Get variants data
        const variants = getVariantsData();
        console.log('Variants data:', variants);
        
        // Get custom fields data
        const customFields = getCustomFieldsData();
        console.log('Custom fields data:', customFields);
        
        const productData = {
            name,
            description,
            price,
            category,
            image,
            variants,
            customFields,
            createdAt: new Date()
        };
        
        console.log('Saving product data:', productData);
        
        const docRef = await addDoc(collection(db, 'products'), productData);
        showNotification('Product added successfully!', 'success');
        form.reset();
        
        // Clear custom fields and variants containers
        const customFieldsContainer = document.getElementById('custom-fields-container');
        const variantsContainer = document.getElementById('variants-container');
        
        if (customFieldsContainer) {
            customFieldsContainer.innerHTML = '';
        }
        
        if (variantsContainer) {
            variantsContainer.innerHTML = '';
        }
        
        loadProducts();
    } catch (error) {
        console.error('Error adding product:', error);
        showNotification('Error adding product: ' + error.message, 'error');
    }
}

// Initialize product form
function initializeProductForm() {
    console.log('Initializing product form');
    
    // Add custom field button
    const addFieldBtn = document.getElementById('add-field-btn');
    if (addFieldBtn) {
        addFieldBtn.addEventListener('click', () => addCustomField('text'));
    } else {
        console.warn('Add field button not found');
    }
    
    // Initialize save product button
    const saveProductBtn = document.getElementById('save-product-btn');
    if (saveProductBtn) {
        saveProductBtn.addEventListener('click', saveProduct);
    } else {
        console.warn('Save product button not found');
    }
    
    // Initialize form submission
    const addProductForm = document.getElementById('add-product-form');
    if (addProductForm) {
        addProductForm.addEventListener('submit', (e) => {
            e.preventDefault();
            saveProduct();
        });
    } else {
        console.warn('Add product form not found');
    }
    
    // Initialize variant functionality
    initializeVariantFunctionality();
}

// Initialize the admin panel when the page loads
document.addEventListener('DOMContentLoaded', initializeAdminPanel);