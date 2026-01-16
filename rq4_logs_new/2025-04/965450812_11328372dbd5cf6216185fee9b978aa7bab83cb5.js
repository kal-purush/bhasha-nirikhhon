// نظام تسجيل الدخول للمدير
const adminCredentials = {
    username: "admin",
    password: "admin123"
};

// تخزين حالة تسجيل الدخول في localStorage
function isAdminLoggedIn() {
    return localStorage.getItem('adminLoggedIn') === 'true';
}

function loginAdmin(username, password) {
    if (username === adminCredentials.username && password === adminCredentials.password) {
        localStorage.setItem('adminLoggedIn', 'true');
        return true;
    }
    return false;
}

function logoutAdmin() {
    localStorage.setItem('adminLoggedIn', 'false');
}

// التحقق من حالة تسجيل الدخول عند تحميل الصفحة
document.addEventListener('DOMContentLoaded', function() {
    // إضافة زر تسجيل الدخول للمدير في أسفل الصفحة
    const footerAdminBtn = document.getElementById('adminPanelBtn');
    
    if (footerAdminBtn) {
        footerAdminBtn.addEventListener('click', function(e) {
            e.preventDefault();
            if (isAdminLoggedIn()) {
                showAdminPanel();
            } else {
                showLoginModal();
            }
        });
    }
});

// إظهار نافذة تسجيل الدخول
function showLoginModal() {
    // إنشاء نافذة تسجيل الدخول
    const loginModal = document.createElement('div');
    loginModal.className = 'modal fade show';
    loginModal.id = 'loginModal';
    loginModal.style.display = 'block';
    loginModal.style.backgroundColor = 'rgba(0,0,0,0.5)';
    
    const currentLang = document.documentElement.lang;
    const isArabic = currentLang === 'ar';
    
    const loginTitle = isArabic ? 'تسجيل دخول المدير' : 'Admin Login';
    const usernamePlaceholder = isArabic ? 'اسم المستخدم' : 'Username';
    const passwordPlaceholder = isArabic ? 'كلمة المرور' : 'Password';
    const loginBtnText = isArabic ? 'تسجيل الدخول' : 'Login';
    const closeBtnText = isArabic ? 'إغلاق' : 'Close';
    
    loginModal.innerHTML = `
        <div class="modal-dialog modal-dialog-centered">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title">${loginTitle}</h5>
                    <button type="button" class="btn-close" aria-label="Close" onclick="document.getElementById('loginModal').remove()"></button>
                </div>
                <div class="modal-body">
                    <form id="adminLoginForm">
                        <div class="mb-3">
                            <label for="username" class="form-label">${usernamePlaceholder}</label>
                            <input type="text" class="form-control" id="username" required>
                        </div>
                        <div class="mb-3">
                            <label for="password" class="form-label">${passwordPlaceholder}</label>
                            <input type="password" class="form-control" id="password" required>
                        </div>
                        <div id="loginError" class="text-danger mb-3" style="display: none;">
                            ${isArabic ? 'اسم المستخدم أو كلمة المرور غير صحيحة' : 'Invalid username or password'}
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" onclick="document.getElementById('loginModal').remove()">${closeBtnText}</button>
                    <button type="button" class="btn btn-primary" id="loginBtn">${loginBtnText}</button>
                </div>
            </div>
        </div>
    `;
    
    document.body.appendChild(loginModal);
    
    // إضافة حدث النقر على زر تسجيل الدخول
    document.getElementById('loginBtn').addEventListener('click', function() {
        const username = document.getElementById('username').value;
        const password = document.getElementById('password').value;
        
        if (loginAdmin(username, password)) {
            document.getElementById('loginModal').remove();
            showAdminPanel();
        } else {
            document.getElementById('loginError').style.display = 'block';
        }
    });
}

// إضافة زر تسجيل الخروج في لوحة التحكم
function addLogoutButton() {
    const currentLang = document.documentElement.lang;
    const isArabic = currentLang === 'ar';
    
    const logoutBtnText = isArabic ? 'تسجيل الخروج' : 'Logout';
    
    const logoutBtn = document.createElement('button');
    logoutBtn.className = 'btn btn-danger ms-2';
    logoutBtn.textContent = logoutBtnText;
    logoutBtn.addEventListener('click', function() {
        logoutAdmin();
        document.getElementById('adminPanel').remove();
    });
    
    const modalHeader = document.querySelector('#adminPanel .modal-header');
    modalHeader.appendChild(logoutBtn);
}