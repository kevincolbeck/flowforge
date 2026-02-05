// Authentication Logic
const Auth = {
    init() {
        // Check if already authenticated
        if (api.isAuthenticated()) {
            this.showApp();
        } else {
            this.showAuth();
        }

        // Setup event listeners
        document.getElementById('show-signup').addEventListener('click', (e) => {
            e.preventDefault();
            this.showSignup();
        });

        document.getElementById('show-login').addEventListener('click', (e) => {
            e.preventDefault();
            this.showLogin();
        });

        document.getElementById('login-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleLogin();
        });

        document.getElementById('signup-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleSignup();
        });

        document.getElementById('logout-btn').addEventListener('click', () => {
            this.handleLogout();
        });
    },

    showAuth() {
        document.getElementById('auth-container').style.display = 'flex';
        document.getElementById('app-container').style.display = 'none';
    },

    showApp() {
        document.getElementById('auth-container').style.display = 'none';
        document.getElementById('app-container').style.display = 'grid';

        // Update user info in sidebar
        if (api.user) {
            document.getElementById('user-name').textContent = api.user.name || 'User';
            document.getElementById('user-email').textContent = api.user.email || '';
        }

        // Load app data
        if (window.App) {
            window.App.loadDashboard();
        }
    },

    showLogin() {
        document.getElementById('login-screen').style.display = 'flex';
        document.getElementById('signup-screen').style.display = 'none';
    },

    showSignup() {
        document.getElementById('login-screen').style.display = 'none';
        document.getElementById('signup-screen').style.display = 'flex';
    },

    async handleLogin() {
        const email = document.getElementById('login-email').value;
        const password = document.getElementById('login-password').value;

        try {
            await api.login(email, password);
            Utils.showToast('Login successful!', 'success');
            this.showApp();
        } catch (error) {
            Utils.showToast(error.message || 'Login failed', 'error');
        }
    },

    async handleSignup() {
        const name = document.getElementById('signup-name').value;
        const email = document.getElementById('signup-email').value;
        const organization = document.getElementById('signup-organization').value || null;
        const password = document.getElementById('signup-password').value;

        try {
            await api.signup(name, email, organization, password);
            Utils.showToast('Account created successfully!', 'success');
            this.showApp();
        } catch (error) {
            Utils.showToast(error.message || 'Signup failed', 'error');
        }
    },

    handleLogout() {
        api.logout();
        Utils.showToast('Logged out successfully', 'success');
        this.showAuth();
        this.showLogin();
    }
};

// Utility Functions
const Utils = {
    showToast(message, type = 'success') {
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `
            <div class="toast-message">${message}</div>
        `;
        container.appendChild(toast);

        setTimeout(() => {
            toast.remove();
        }, 4000);
    },

    formatDate(dateString) {
        if (!dateString) return 'N/A';
        const date = new Date(dateString);
        return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
    },

    formatDuration(ms) {
        if (!ms) return 'N/A';
        const seconds = Math.floor(ms / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);

        if (hours > 0) return `${hours}h ${minutes % 60}m`;
        if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
        return `${seconds}s`;
    }
};
