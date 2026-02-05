// API Client for FlowForge
class APIClient {
    constructor() {
        this.baseURL = window.location.origin;
        this.token = localStorage.getItem('auth_token');
        this.user = JSON.parse(localStorage.getItem('user') || 'null');
    }

    async request(endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const headers = {
            'Content-Type': 'application/json',
            ...options.headers,
        };

        if (this.token) {
            headers['X-API-Key'] = this.token;
        }

        try {
            const response = await fetch(url, {
                ...options,
                headers,
            });

            if (!response.ok) {
                const error = await response.json().catch(() => ({ detail: response.statusText }));
                throw new Error(error.detail || error.message || 'Request failed');
            }

            return await response.json();
        } catch (error) {
            console.error('API Request failed:', error);
            throw error;
        }
    }

    // Authentication
    async signup(name, email, organization, password) {
        const data = await this.request('/auth/signup', {
            method: 'POST',
            body: JSON.stringify({ name, email, organization, password }),
        });

        if (data.api_key) {
            this.setAuth(data.api_key, data.user);
        }

        return data;
    }

    async login(email, password) {
        const data = await this.request('/auth/login', {
            method: 'POST',
            body: JSON.stringify({ email, password }),
        });

        if (data.api_key) {
            this.setAuth(data.api_key, data.user);
        }

        return data;
    }

    setAuth(token, user) {
        this.token = token;
        this.user = user;
        localStorage.setItem('auth_token', token);
        localStorage.setItem('user', JSON.stringify(user));
    }

    logout() {
        this.token = null;
        this.user = null;
        localStorage.removeItem('auth_token');
        localStorage.removeItem('user');
    }

    isAuthenticated() {
        return !!this.token;
    }

    // Workflows
    async getWorkflows() {
        return this.request('/workflows');
    }

    async getWorkflow(id) {
        return this.request(`/workflows/${id}`);
    }

    async createWorkflow(workflow) {
        return this.request('/workflows', {
            method: 'POST',
            body: JSON.stringify(workflow),
        });
    }

    async updateWorkflow(id, workflow) {
        return this.request(`/workflows/${id}`, {
            method: 'PUT',
            body: JSON.stringify(workflow),
        });
    }

    async deleteWorkflow(id) {
        return this.request(`/workflows/${id}`, {
            method: 'DELETE',
        });
    }

    async executeWorkflow(id, triggerData = {}) {
        return this.request(`/workflows/${id}/execute`, {
            method: 'POST',
            body: JSON.stringify({ trigger_data: triggerData }),
        });
    }

    // Credentials
    async getCredentials(service = null) {
        const query = service ? `?service=${service}` : '';
        return this.request(`/credentials${query}`);
    }

    async createCredential(credential) {
        return this.request('/credentials', {
            method: 'POST',
            body: JSON.stringify(credential),
        });
    }

    async deleteCredential(id) {
        return this.request(`/credentials/${id}`, {
            method: 'DELETE',
        });
    }

    // Services & Connectors
    async getServices(category = null, search = null) {
        const params = new URLSearchParams();
        if (category) params.append('category', category);
        if (search) params.append('search', search);
        const query = params.toString() ? `?${params}` : '';
        return this.request(`/services${query}`);
    }

    async getService(name) {
        return this.request(`/services/${name}`);
    }

    async getConnectors() {
        return this.request('/api/connectors');
    }

    async getConnector(service) {
        return this.request(`/api/connectors/${service}`);
    }

    // Templates
    async getTemplates(category = null) {
        const query = category ? `?category=${category}` : '';
        return this.request(`/templates${query}`);
    }

    async getTemplate(id) {
        return this.request(`/templates/${id}`);
    }

    async createFromTemplate(id, config = {}) {
        return this.request(`/templates/${id}/create`, {
            method: 'POST',
            body: JSON.stringify(config),
        });
    }

    // Executions
    async getRuns(workflowId = null, limit = 50) {
        const params = new URLSearchParams({ limit: limit.toString() });
        if (workflowId) params.append('workflow_id', workflowId);
        return this.request(`/runs?${params}`);
    }

    async getRun(id) {
        return this.request(`/runs/${id}`);
    }

    async getRunLogs(id) {
        return this.request(`/runs/${id}/logs`);
    }

    async getWorkflowStats(workflowId) {
        return this.request(`/workflows/${workflowId}/stats`);
    }

    // System
    async getHealth() {
        return this.request('/health');
    }

    async getStatus() {
        return this.request('/status');
    }
}

// Global API instance
window.api = new APIClient();
