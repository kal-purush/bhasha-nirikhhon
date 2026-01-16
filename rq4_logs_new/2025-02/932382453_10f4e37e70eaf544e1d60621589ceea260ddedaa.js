// app.js
import { TimelineManager } from './components/timeline/timeline-manager.js';
import { TimelineRecord } from './components/timeline/timeline-record.js';
import { TextEditor } from './components/editors/text-editor.js';
import { AudioEditor } from './components/editors/audio-editor.js';
import { ImageEditor } from './components/editors/image-editor.js';
import { StorageManager } from './utils/storage.js';
import { MediaHandler } from './utils/media-handlers.js';


class App {
    constructor() {
        // Ensure all custom elements are defined before initializing
 
        this.storage = new StorageManager();
    
        this.currentTimelineId = null;
        this.initializeElements();
        this.initializeEventListeners();
        this.initializeCustomElements();
    }

    initializeElements() {
        // Main navigation elements
        this.newTimelineBtn = document.getElementById('newTimelineBtn');
        this.importBtn = document.getElementById('importBtn');
        this.exportBtn = document.getElementById('exportBtn');
        this.newRecordBtn = document.getElementById('newRecordBtn');
        
        // Dialog elements
        this.newRecordDialog = document.getElementById('newRecordDialog');
        
        // Timeline elements
        this.timelineNav = document.getElementById('timelineNav');
        this.timelineManager = document.getElementById('timelineManager');
        this.timelineTitle = document.getElementById('timelineTitle');
        
        // Initialize state
        this.newRecordBtn.disabled = true;
        this.exportBtn.disabled = true;
    }

    initializeEventListeners() {
        // Main navigation events
        this.newTimelineBtn.addEventListener('click', () => this.createNewTimeline());
        this.importBtn.addEventListener('click', () => this.importTimeline());
        this.exportBtn.addEventListener('click', () => this.exportTimeline());
        this.newRecordBtn.addEventListener('click', () => this.showNewRecordDialog());
        
        // Dialog events
        this.newRecordDialog.addEventListener('close', () => this.handleDialogClose());
        
        // Record type selection
        const recordTypeInputs = this.newRecordDialog.querySelectorAll('input[name="recordType"]');
        recordTypeInputs.forEach(input => {
            input.addEventListener('change', (e) => this.handleRecordTypeChange(e.target.value));
        });
        
        // Online/Offline events
        window.addEventListener('online', () => this.handleOnlineStatus(true));
        window.addEventListener('offline', () => this.handleOnlineStatus(false));
        
        // Handle record events
        document.addEventListener('save-record', (e) => this.handleSaveRecord(e));
        document.addEventListener('edit-record', (e) => this.handleEditRecord(e));
        document.addEventListener('record-updated', (e) => this.handleRecordUpdate(e));
// Add form submit handler
const form = this.newRecordDialog.querySelector('form');
form.addEventListener('submit', async (e) => {
    e.preventDefault(); // Prevent the dialog from closing automatically
    console.log('Form submitted');
    
    const editor = this.newRecordDialog.querySelector('.editor-container > *');
    if (editor && typeof editor.save === 'function') {
        console.log('Calling editor save method');
        const success = await editor.save();
        if (success) {
            console.log('Editor save successful');
            // Dialog will be closed in handleSaveRecord
        }
    } else {
        console.error('No editor found or save method not available');
    }
});

// Update dialog close button handler
const closeButton = this.newRecordDialog.querySelector('.close-button');
closeButton.addEventListener('click', () => {
    console.log('Close button clicked');
    this.newRecordDialog.close();
});

// Update cancel button handler
const cancelButton = this.newRecordDialog.querySelector('button.secondary');
cancelButton.addEventListener('click', () => {
    console.log('Cancel button clicked');
    this.newRecordDialog.close();
});
}

    initializeCustomElements() {
        // Register custom elements if not already registered
        if (!customElements.get('timeline-manager')) {
            customElements.define('timeline-manager', TimelineManager);
        }
        if (!customElements.get('timeline-record')) {
            customElements.define('timeline-record', TimelineRecord);
        }
        if (!customElements.get('text-editor')) {
            customElements.define('text-editor', TextEditor);
        }
        if (!customElements.get('audio-editor')) {
            customElements.define('audio-editor', AudioEditor);
        }
        if (!customElements.get('image-editor')) {
            customElements.define('image-editor', ImageEditor);
        }
    }

    async createNewTimeline() {
        const name = prompt('Enter timeline name:');
        if (!name) return;
        
        try {
            const timeline = await this.storage.createTimeline(name);
            await this.loadTimelines();
            await this.loadTimeline(timeline.id);
            this.showNotification('Timeline created successfully', 'success');
        } catch (error) {
            console.error('Error creating timeline:', error);
            this.showNotification('Failed to create timeline', 'error');
        }
    }

    async importTimeline() {
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.json';
        
        input.onchange = async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            
            try {
                const text = await file.text();
                const timelineData = JSON.parse(text);
                const timeline = await this.storage.importTimeline(timelineData);
                await this.loadTimelines();
                await this.loadTimeline(timeline.id);
                this.showNotification('Timeline imported successfully', 'success');
            } catch (error) {
                console.error('Error importing timeline:', error);
                this.showNotification('Failed to import timeline', 'error');
            }
        };
        
        input.click();
    }

    async handleSaveRecord(event) {
        const { content, type } = event.detail;
        if (!this.currentTimelineId) return;
        
        try {
            await this.storage.addRecord(this.currentTimelineId, { type, content });
            this.newRecordDialog.close();
            
            // Make sure to reload the timeline manager's content
            const timelineManager = document.getElementById('timelineManager');
            await timelineManager.loadTimeline(this.currentTimelineId);
            
            this.showNotification('Record saved successfully', 'success');
        } catch (error) {
            console.error('Error saving record:', error);
            this.showNotification('Failed to save record', 'error');
        }
    }

    async exportTimeline() {
        if (!this.currentTimelineId) return;
        
        try {
            const data = await this.storage.exportTimeline(this.currentTimelineId);
            const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            
            const a = document.createElement('a');
            a.href = url;
            a.download = `timeline-${data.metadata.name}-${new Date().toISOString()}.json`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            
            this.showNotification('Timeline exported successfully', 'success');
        } catch (error) {
            console.error('Error exporting timeline:', error);
            this.showNotification('Failed to export timeline', 'error');
        }
    }

showNewRecordDialog() {
    console.log('Opening new record dialog');
    const dialog = this.newRecordDialog;
    dialog.querySelector('form').reset();
    
    // Initialize with text editor
    this.handleRecordTypeChange('text');
    
    dialog.showModal();
}

handleRecordTypeChange(type) {
    console.log('Changing record type to:', type);
    const container = this.newRecordDialog.querySelector('.editor-container');
    container.innerHTML = '';
    
    const editor = document.createElement(`${type}-editor`);
    container.appendChild(editor);
}

    async handleSaveRecord(event) {
        const { content, type } = event.detail;
        if (!this.currentTimelineId) return;
        
        try {
            await this.storage.addRecord(this.currentTimelineId, { type, content });
            this.newRecordDialog.close();
            await this.loadTimeline(this.currentTimelineId);
            this.showNotification('Record saved successfully', 'success');
        } catch (error) {
            console.error('Error saving record:', error);
            this.showNotification('Failed to save record', 'error');
        }
    }

    async handleEditRecord(event) {
        const { record, timelineId } = event.detail;
        const dialog = this.newRecordDialog;
        
        // Set record type
        dialog.querySelector(`input[value="${record.type}"]`).checked = true;
        
        // Initialize editor
        this.handleRecordTypeChange(record.type);
        const editor = dialog.querySelector(`${record.type}-editor`);
        editor.setAttribute('content', record.content);
        
        // Store record ID for update
        dialog.dataset.recordId = record.id;
        dialog.dataset.timelineId = timelineId;
        
        dialog.showModal();
    }

    async handleRecordUpdate(event) {
        const { record, timelineId, action } = event.detail;
        
        try {
            if (action === 'delete') {
                await this.storage.deleteRecord(timelineId, record.id);
                this.showNotification('Record deleted successfully', 'success');
            } else if (action === 'update') {
                await this.storage.updateRecord(timelineId, record.id, record);
                this.showNotification('Record updated successfully', 'success');
            }
            
            await this.loadTimeline(timelineId);
        } catch (error) {
            console.error('Error updating record:', error);
            this.showNotification('Failed to update record', 'error');
        }
    }

    handleDialogClose() {
        // Clean up any media streams or resources
        const editor = this.newRecordDialog.querySelector('.editor-container > *');
        if (editor && typeof editor.cleanup === 'function') {
            editor.cleanup();
        }
    }

    async loadTimelines() {
        try {
            const timelines = await this.storage.getTimelines();
            this.timelineNav.innerHTML = '';
            
            timelines.forEach(timeline => {
                const button = document.createElement('button');
                button.textContent = timeline.name;
                button.addEventListener('click', () => this.loadTimeline(timeline.id));
                if (timeline.id === this.currentTimelineId) {
                    button.classList.add('active');
                }
                this.timelineNav.appendChild(button);
            });
        } catch (error) {
            console.error('Error loading timelines:', error);
            this.showNotification('Failed to load timelines', 'error');
        }
    }

    async loadTimeline(timelineId) {
        try {
            const timeline = await this.storage.getTimelineStore(timelineId);
            this.currentTimelineId = timelineId;
            this.timelineTitle.textContent = timeline.name;
            this.newRecordBtn.disabled = false;
            this.exportBtn.disabled = false;
            
            await this.timelineManager.loadTimeline(timelineId);
            
            // Update active state in navigation
            const buttons = this.timelineNav.querySelectorAll('button');
            buttons.forEach(button => {
                button.classList.toggle('active', button.textContent === timeline.name);
            });
        } catch (error) {
            console.error('Error loading timeline:', error);
            this.showNotification('Failed to load timeline', 'error');
        }
    }

    handleOnlineStatus(isOnline) {
        const status = document.createElement('div');
        status.className = `status-message ${isOnline ? 'success' : 'error'}`;
        status.textContent = isOnline ? 'Back online' : 'You are offline. Changes will be synced when connection is restored.';
        
        document.body.appendChild(status);
        setTimeout(() => status.remove(), 3000);
    }

    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.textContent = message;
        
        document.body.appendChild(notification);
        setTimeout(() => notification.remove(), 3000);
    }
}

// Initialize app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.app = new App();
    // Load existing timelines
    window.app.loadTimelines();
});

// Export for module usage
export default App;