// Main Application Logic
const App = {
    workflows: [],
    credentials: [],
    executions: [],
    templates: [],

    init() {
        // Initialize authentication
        Auth.init();

        // Initialize workflow builder
        WorkflowBuilder.init();

        // Setup navigation
        document.querySelectorAll('.nav-link').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const view = e.target.getAttribute('data-view');
                this.showView(view);
            });
        });

        // Setup create workflow buttons
        document.getElementById('create-workflow-btn').addEventListener('click', () => {
            WorkflowBuilder.newWorkflow();
        });

        document.getElementById('create-workflow-btn-2').addEventListener('click', () => {
            WorkflowBuilder.newWorkflow();
        });

        // Setup credentials
        document.getElementById('add-credential-btn').addEventListener('click', () => {
            this.showCredentialModal();
        });

        document.getElementById('cancel-cred-btn').addEventListener('click', () => {
            this.hideCredentialModal();
        });

        document.getElementById('credential-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.addCredential();
        });

        // Close modals on background click
        document.getElementById('step-modal').addEventListener('click', (e) => {
            if (e.target.id === 'step-modal') {
                WorkflowBuilder.hideStepModal();
            }
        });

        document.getElementById('credential-modal').addEventListener('click', (e) => {
            if (e.target.id === 'credential-modal') {
                this.hideCredentialModal();
            }
        });
    },

    showView(viewName) {
        // Update nav links
        document.querySelectorAll('.nav-link').forEach(link => {
            link.classList.remove('active');
            if (link.getAttribute('data-view') === viewName) {
                link.classList.add('active');
            }
        });

        // Show view
        document.querySelectorAll('.view').forEach(view => {
            view.classList.remove('active');
        });

        const targetView = document.getElementById(`${viewName}-view`);
        if (targetView) {
            targetView.classList.add('active');
        }

        // Load data for view
        switch (viewName) {
            case 'dashboard':
                this.loadDashboard();
                break;
            case 'workflows':
                this.loadWorkflows();
                break;
            case 'credentials':
                this.loadCredentials();
                break;
            case 'executions':
                this.loadExecutions();
                break;
            case 'templates':
                this.loadTemplates();
                break;
        }
    },

    async loadDashboard() {
        try {
            const [workflowsData, runsData] = await Promise.all([
                api.getWorkflows(),
                api.getRuns(null, 10)
            ]);

            this.workflows = workflowsData.workflows || [];
            this.executions = runsData.runs || [];

            // Update stats
            const activeWorkflows = this.workflows.filter(w => w.status === 'active').length;
            const successfulRuns = this.executions.filter(e => e.status === 'success').length;
            const successRate = this.executions.length > 0
                ? Math.round((successfulRuns / this.executions.length) * 100)
                : 0;

            document.getElementById('total-workflows').textContent = this.workflows.length;
            document.getElementById('active-workflows').textContent = activeWorkflows;
            document.getElementById('total-executions').textContent = this.executions.length;
            document.getElementById('success-rate').textContent = successRate + '%';

            // Render recent workflows
            this.renderRecentWorkflows();
        } catch (error) {
            console.error('Failed to load dashboard:', error);
            Utils.showToast('Failed to load dashboard data', 'error');
        }
    },

    renderRecentWorkflows() {
        const container = document.getElementById('recent-workflows');
        container.innerHTML = '';

        if (this.workflows.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No workflows yet</h3>
                    <p>Create your first workflow to get started</p>
                    <button class="btn btn-primary" onclick="WorkflowBuilder.newWorkflow()">Create Workflow</button>
                </div>
            `;
            return;
        }

        const recentWorkflows = this.workflows.slice(0, 5);
        recentWorkflows.forEach(workflow => {
            this.renderWorkflowCard(container, workflow);
        });
    },

    async loadWorkflows() {
        try {
            const data = await api.getWorkflows();
            this.workflows = data.workflows || [];
            this.renderWorkflows();
        } catch (error) {
            console.error('Failed to load workflows:', error);
            Utils.showToast('Failed to load workflows', 'error');
        }
    },

    renderWorkflows() {
        const container = document.getElementById('workflows-list');
        container.innerHTML = '';

        if (this.workflows.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No workflows yet</h3>
                    <p>Create your first workflow to automate your integrations</p>
                    <button class="btn btn-primary" onclick="WorkflowBuilder.newWorkflow()">Create Workflow</button>
                </div>
            `;
            return;
        }

        this.workflows.forEach(workflow => {
            this.renderWorkflowCard(container, workflow);
        });
    },

    renderWorkflowCard(container, workflow) {
        const card = document.createElement('div');
        card.className = 'workflow-card';

        const statusClass = `status-${workflow.status}`;
        const stepsCount = workflow.steps?.length || 0;

        card.innerHTML = `
            <div class="workflow-info">
                <h3>${workflow.name}</h3>
                <p>${workflow.description || 'No description'}</p>
                <div class="workflow-meta">
                    <span class="status-badge ${statusClass}">${workflow.status}</span>
                    <span>${stepsCount} steps</span>
                    <span>Created: ${Utils.formatDate(workflow.created_at)}</span>
                </div>
            </div>
            <div class="workflow-actions">
                <button class="btn btn-secondary" onclick="WorkflowBuilder.editWorkflow('${workflow.id}')">Edit</button>
                <button class="btn btn-success" onclick="App.runWorkflow('${workflow.id}')">Run</button>
                <button class="btn btn-danger" onclick="App.deleteWorkflow('${workflow.id}')">Delete</button>
            </div>
        `;

        container.appendChild(card);
    },

    async runWorkflow(id) {
        try {
            await api.executeWorkflow(id);
            Utils.showToast('Workflow execution started', 'success');
            setTimeout(() => this.loadExecutions(), 1000);
        } catch (error) {
            Utils.showToast('Failed to execute workflow: ' + error.message, 'error');
        }
    },

    async deleteWorkflow(id) {
        if (!confirm('Are you sure you want to delete this workflow?')) {
            return;
        }

        try {
            await api.deleteWorkflow(id);
            Utils.showToast('Workflow deleted successfully', 'success');
            this.loadWorkflows();
            this.loadDashboard();
        } catch (error) {
            Utils.showToast('Failed to delete workflow: ' + error.message, 'error');
        }
    },

    async loadCredentials() {
        try {
            const data = await api.getCredentials();
            this.credentials = data.credentials || [];
            this.renderCredentials();
        } catch (error) {
            console.error('Failed to load credentials:', error);
            Utils.showToast('Failed to load credentials', 'error');
        }
    },

    renderCredentials() {
        const container = document.getElementById('credentials-list');
        container.innerHTML = '';

        if (this.credentials.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No credentials yet</h3>
                    <p>Add credentials to connect with external services</p>
                </div>
            `;
            return;
        }

        this.credentials.forEach(credential => {
            const card = document.createElement('div');
            card.className = 'credential-card';

            card.innerHTML = `
                <div class="credential-info">
                    <h3>${credential.name}</h3>
                    <p>Service: ${credential.service} | Type: ${credential.credential_type}</p>
                    <p class="workflow-meta">Created: ${Utils.formatDate(credential.created_at)}</p>
                </div>
                <div class="workflow-actions">
                    <button class="btn btn-danger" onclick="App.deleteCredential('${credential.id}')">Delete</button>
                </div>
            `;

            container.appendChild(card);
        });
    },

    showCredentialModal() {
        document.getElementById('credential-modal').classList.add('active');
    },

    hideCredentialModal() {
        document.getElementById('credential-modal').classList.remove('active');
        document.getElementById('credential-form').reset();
    },

    async addCredential() {
        const name = document.getElementById('cred-name').value.trim();
        const service = document.getElementById('cred-service').value.trim();
        const type = document.getElementById('cred-type').value;
        const dataText = document.getElementById('cred-data').value;

        if (!name || !service || !type) {
            Utils.showToast('Please fill all required fields', 'error');
            return;
        }

        let data = {};
        try {
            data = JSON.parse(dataText);
        } catch (error) {
            Utils.showToast('Invalid JSON data', 'error');
            return;
        }

        try {
            await api.createCredential({
                name,
                service,
                credential_type: type,
                data
            });

            Utils.showToast('Credential added successfully', 'success');
            this.hideCredentialModal();
            this.loadCredentials();
        } catch (error) {
            Utils.showToast('Failed to add credential: ' + error.message, 'error');
        }
    },

    async deleteCredential(id) {
        if (!confirm('Are you sure you want to delete this credential?')) {
            return;
        }

        try {
            await api.deleteCredential(id);
            Utils.showToast('Credential deleted successfully', 'success');
            this.loadCredentials();
        } catch (error) {
            Utils.showToast('Failed to delete credential: ' + error.message, 'error');
        }
    },

    async loadExecutions() {
        try {
            const data = await api.getRuns(null, 100);
            this.executions = data.runs || [];
            this.renderExecutions();
        } catch (error) {
            console.error('Failed to load executions:', error);
            Utils.showToast('Failed to load executions', 'error');
        }
    },

    renderExecutions() {
        const container = document.getElementById('executions-list');
        container.innerHTML = '';

        if (this.executions.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No executions yet</h3>
                    <p>Run a workflow to see execution history</p>
                </div>
            `;
            return;
        }

        this.executions.forEach(execution => {
            const item = document.createElement('div');
            item.className = 'execution-item';

            const statusClass = `status-${execution.status}`;
            const workflow = this.workflows.find(w => w.id === execution.workflow_id);
            const workflowName = workflow?.name || 'Unknown Workflow';

            item.innerHTML = `
                <div class="execution-info">
                    <h4>${workflowName}</h4>
                    <p>Started: ${Utils.formatDate(execution.started_at)}</p>
                    ${execution.completed_at ? `<p>Duration: ${Utils.formatDuration(execution.duration_ms)}</p>` : ''}
                </div>
                <div class="execution-status">
                    <span class="status-badge ${statusClass}">${execution.status}</span>
                </div>
            `;

            container.appendChild(item);
        });
    },

    async loadTemplates() {
        try {
            const data = await api.getTemplates();
            this.templates = data.templates || [];
            this.renderTemplates();
        } catch (error) {
            console.error('Failed to load templates:', error);
            Utils.showToast('Failed to load templates', 'error');
        }
    },

    renderTemplates() {
        const container = document.getElementById('templates-list');
        container.innerHTML = '';

        if (this.templates.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No templates available</h3>
                    <p>Templates will help you get started quickly</p>
                </div>
            `;
            return;
        }

        this.templates.forEach(template => {
            const card = document.createElement('div');
            card.className = 'template-card';

            card.innerHTML = `
                <h3>${template.name}</h3>
                <p>${template.description || 'No description'}</p>
                <div>
                    ${template.category ? `<span class="template-tag">${template.category}</span>` : ''}
                    <span class="template-tag">${template.steps?.length || 0} steps</span>
                </div>
                <button class="btn btn-primary" onclick="App.useTemplate('${template.id}')">Use Template</button>
            `;

            container.appendChild(card);
        });
    },

    async useTemplate(id) {
        try {
            const result = await api.createFromTemplate(id);
            Utils.showToast('Workflow created from template', 'success');

            if (result.workflow) {
                WorkflowBuilder.editWorkflow(result.workflow.id);
            }
        } catch (error) {
            Utils.showToast('Failed to create from template: ' + error.message, 'error');
        }
    }
};

// Expose to window
window.App = App;

// Initialize app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    App.init();
});
