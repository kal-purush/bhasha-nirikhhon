const SUPABASE_URL = 'YOUR_SUPABASE_URL';
const SUPABASE_ANON_KEY = 'YOUR_SUPABASE_ANON_KEY';

const supabase = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

class Auth {
    constructor() {
        this.currentUser = null;
        this.initializeElements();
        this.addEventListeners();
        this.checkUser();
    }

    initializeElements() {
        // Auth sections
        this.authSection = document.getElementById('auth-section');
        this.userSection = document.getElementById('user-section');
        this.usernameDisplay = document.getElementById('username-display');
        
        // Buttons
        this.loginBtn = document.getElementById('login-btn');
        this.registerBtn = document.getElementById('register-btn');
        this.logoutBtn = document.getElementById('logout-btn');
        
        // Modal elements
        this.authModal = document.getElementById('auth-modal');
        this.loginForm = document.getElementById('login-form');
        this.registerForm = document.getElementById('register-form');
    }

    addEventListeners() {
        this.loginBtn.addEventListener('click', () => this.showAuthModal('login'));
        this.registerBtn.addEventListener('click', () => this.showAuthModal('register'));
        this.logoutBtn.addEventListener('click', () => this.logout());

        document.getElementById('submit-login').addEventListener('click', () => this.login());
        document.getElementById('submit-register').addEventListener('click', () => this.register());

        // Close modal when clicking outside
        this.authModal.addEventListener('click', (e) => {
            if (e.target === this.authModal) {
                this.hideAuthModal();
            }
        });
    }

    async checkUser() {
        const { data: { user }, error } = await supabase.auth.getUser();
        if (user) {
            this.currentUser = user;
            this.updateUIForLoggedInUser();
        }
    }

    showAuthModal(type) {
        this.authModal.classList.remove('hidden');
        if (type === 'login') {
            this.loginForm.classList.remove('hidden');
            this.registerForm.classList.add('hidden');
        } else {
            this.loginForm.classList.add('hidden');
            this.registerForm.classList.remove('hidden');
        }
    }

    hideAuthModal() {
        this.authModal.classList.add('hidden');
    }

    async login() {
        const email = document.getElementById('login-email').value;
        const password = document.getElementById('login-password').value;

        const { data, error } = await supabase.auth.signInWithPassword({
            email,
            password
        });

        if (error) {
            alert(error.message);
        } else {
            this.currentUser = data.user;
            this.updateUIForLoggedInUser();
            this.hideAuthModal();
        }
    }

    async register() {
        const email = document.getElementById('register-email').value;
        const password = document.getElementById('register-password').value;
        const username = document.getElementById('register-username').value;

        const { data, error } = await supabase.auth.signUp({
            email,
            password,
            options: {
                data: {
                    username
                }
            }
        });

        if (error) {
            alert(error.message);
        } else {
            alert('Registration successful! Please check your email for verification.');
            this.hideAuthModal();
        }
    }

    async logout() {
        const { error } = await supabase.auth.signOut();
        if (error) {
            alert(error.message);
        } else {
            this.currentUser = null;
            this.updateUIForLoggedOutUser();
        }
    }

    updateUIForLoggedInUser() {
        this.authSection.classList.add('hidden');
        this.userSection.classList.remove('hidden');
        this.usernameDisplay.textContent = this.currentUser.user_metadata.username;
        document.getElementById('new-thread-btn').classList.remove('hidden');
        document.querySelector('.post-controls').classList.remove('hidden');
    }

    updateUIForLoggedOutUser() {
        this.authSection.classList.remove('hidden');
        this.userSection.classList.add('hidden');
        document.getElementById('new-thread-btn').classList.add('hidden');
        document.querySelector('.post-controls').classList.add('hidden');
    }
}

const auth = new Auth(); 