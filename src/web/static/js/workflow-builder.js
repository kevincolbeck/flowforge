// Workflow Builder Logic
const WorkflowBuilder = {
    currentWorkflow: null,
    currentSteps: [],
    availableServices: [],
    availableConnectors: [],

    async init() {
        // Load available services and connectors
        try {
            const [servicesData, connectorsData] = await Promise.all([
                api.getServices(),
                api.getConnectors()
            ]);

            this.availableServices = servicesData.services || [];
            this.availableConnectors = connectorsData.connectors || [];

            this.populateServiceDropdown();
        } catch (error) {
            console.error('Failed to load services:', error);
        }

        // Setup event listeners
        document.getElementById('add-step-btn').addEventListener('click', () => {
            this.showStepModal();
        });

        document.getElementById('cancel-step-btn').addEventListener('click', () => {
            this.hideStepModal();
        });

        document.getElementById('step-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.addStep();
        });

        document.getElementById('step-service').addEventListener('change', (e) => {
            this.updateActionDropdown(e.target.value);
        });

        document.getElementById('save-workflow-btn').addEventListener('click', () => {
            this.saveWorkflow(false);
        });

        document.getElementById('save-run-workflow-btn').addEventListener('click', () => {
            this.saveWorkflow(true);
        });

        document.getElementById('cancel-workflow-btn').addEventListener('click', () => {
            window.App.showView('workflows');
        });
    },

    populateServiceDropdown() {
        const select = document.getElementById('step-service');
        select.innerHTML = '<option value="">Select a service...</option>';

        // Add connectors
        this.availableConnectors.forEach(connector => {
            const option = document.createElement('option');
            option.value = connector.name;
            option.textContent = connector.display_name || connector.name;
            select.appendChild(option);
        });
    },

    updateActionDropdown(service) {
        const select = document.getElementById('step-action');
        select.innerHTML = '<option value="">Select an action...</option>';

        const connector = this.availableConnectors.find(c => c.name === service);
        if (connector && connector.actions) {
            connector.actions.forEach(action => {
                const option = document.createElement('option');
                option.value = action.name;
                option.textContent = action.display_name || action.name;
                select.appendChild(option);
            });
        }
    },

    showStepModal() {
        document.getElementById('step-modal').classList.add('active');
    },

    hideStepModal() {
        document.getElementById('step-modal').classList.remove('active');
        document.getElementById('step-form').reset();
    },

    addStep() {
        const service = document.getElementById('step-service').value;
        const action = document.getElementById('step-action').value;
        const configText = document.getElementById('step-config').value;

        if (!service || !action) {
            Utils.showToast('Please select service and action', 'error');
            return;
        }

        let config = {};
        try {
            config = JSON.parse(configText);
        } catch (error) {
            Utils.showToast('Invalid JSON configuration', 'error');
            return;
        }

        const step = {
            id: `step_${Date.now()}`,
            service,
            action,
            config
        };

        this.currentSteps.push(step);
        this.renderSteps();
        this.hideStepModal();
    },

    removeStep(stepId) {
        this.currentSteps = this.currentSteps.filter(s => s.id !== stepId);
        this.renderSteps();
    },

    renderSteps() {
        const container = document.getElementById('workflow-steps');
        container.innerHTML = '';

        if (this.currentSteps.length === 0) {
            container.innerHTML = '<p class="empty-state">No steps added yet. Click "Add Step" to begin.</p>';
            return;
        }

        this.currentSteps.forEach((step, index) => {
            const stepCard = document.createElement('div');
            stepCard.className = 'step-card';
            stepCard.innerHTML = `
                <div class="step-info">
                    <h4>Step ${index + 1}: ${step.service} - ${step.action}</h4>
                    <p>${JSON.stringify(step.config)}</p>
                </div>
                <div class="step-actions">
                    <button class="btn btn-danger" onclick="WorkflowBuilder.removeStep('${step.id}')">Remove</button>
                </div>
            `;
            container.appendChild(stepCard);
        });
    },

    newWorkflow() {
        this.currentWorkflow = null;
        this.currentSteps = [];

        document.getElementById('builder-title').textContent = 'Create Workflow';
        document.getElementById('workflow-name').value = '';
        document.getElementById('workflow-description').value = '';
        document.getElementById('workflow-trigger').value = 'manual';
        document.getElementById('workflow-status').value = 'draft';

        this.renderSteps();
        window.App.showView('builder');
    },

    async editWorkflow(id) {
        try {
            const data = await api.getWorkflow(id);
            this.currentWorkflow = data.workflow;
            this.currentSteps = this.currentWorkflow.steps || [];

            document.getElementById('builder-title').textContent = 'Edit Workflow';
            document.getElementById('workflow-name').value = this.currentWorkflow.name;
            document.getElementById('workflow-description').value = this.currentWorkflow.description || '';
            document.getElementById('workflow-trigger').value = this.currentWorkflow.trigger?.type || 'manual';
            document.getElementById('workflow-status').value = this.currentWorkflow.status;

            this.renderSteps();
            window.App.showView('builder');
        } catch (error) {
            Utils.showToast('Failed to load workflow: ' + error.message, 'error');
        }
    },

    async saveWorkflow(andRun = false) {
        const name = document.getElementById('workflow-name').value.trim();
        const description = document.getElementById('workflow-description').value.trim();
        const triggerType = document.getElementById('workflow-trigger').value;
        const status = document.getElementById('workflow-status').value;

        if (!name) {
            Utils.showToast('Please enter a workflow name', 'error');
            return;
        }

        if (this.currentSteps.length === 0) {
            Utils.showToast('Please add at least one step', 'error');
            return;
        }

        const workflow = {
            name,
            description,
            trigger: { type: triggerType },
            steps: this.currentSteps,
            status
        };

        try {
            let result;
            if (this.currentWorkflow) {
                result = await api.updateWorkflow(this.currentWorkflow.id, workflow);
                Utils.showToast('Workflow updated successfully', 'success');
            } else {
                result = await api.createWorkflow(workflow);
                Utils.showToast('Workflow created successfully', 'success');
            }

            if (andRun && result.workflow) {
                await api.executeWorkflow(result.workflow.id);
                Utils.showToast('Workflow execution started', 'success');
            }

            window.App.loadWorkflows();
            window.App.showView('workflows');
        } catch (error) {
            Utils.showToast('Failed to save workflow: ' + error.message, 'error');
        }
    }
};

// Expose to window
window.WorkflowBuilder = WorkflowBuilder;
