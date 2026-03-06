let statuses = [];
let editingStatusId = null;
let draggedElement = null;

document.addEventListener('DOMContentLoaded', function() {
    console.log('Status Management page loaded');
    loadStatuses();
    setupEventListeners();
});

function setupEventListeners() {
    console.log('Setting up event listeners');
    
    // Main buttons
    document.getElementById('add-status').addEventListener('click', () => openStatusPopup());
    document.getElementById('save-order').addEventListener('click', saveOrder);
    
    // Popup controls - using event delegation
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('close-popup')) {
            const popupName = e.target.dataset.popup;
            closePopup(popupName);
        }
    });
    
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('btn-cancel') && e.target.dataset.popup) {
            closePopup(e.target.dataset.popup);
        }
    });
    
    // Overlay clicks
    document.getElementById('status-popup-overlay').addEventListener('click', () => closePopup('status-popup'));
    document.getElementById('delete-popup-overlay').addEventListener('click', () => closePopup('delete-popup'));
    
    // Save/Delete actions
    document.getElementById('save-status').addEventListener('click', saveStatus);
    document.getElementById('confirm-delete').addEventListener('click', deleteStatus);
}

async function loadStatuses() {
    try {
        const response = await fetch('/api/statuses');
        const data = await response.json();
        statuses = data.statuses;
        console.log('Loaded', statuses.length, 'statuses');
        renderStatusList();
    } catch (error) {
        console.error('Error loading statuses:', error);
    }
}

function renderStatusList() {
    const container = document.getElementById('status-list');
    container.innerHTML = '';
    
    statuses.forEach((status, index) => {
        const item = document.createElement('div');
        item.className = 'status-item';
        item.draggable = true;
        item.dataset.statusId = status.status_id;
        
        item.innerHTML = `
            <div class="status-color" style="background-color: ${status.s_color}"></div>
            <div class="status-name">${status.s_status}</div>
            <div class="status-controls">
                <button class="btn-edit" data-action="edit" data-status-id="${status.status_id}">Edit</button>
                <button class="btn-delete" data-action="delete" data-status-id="${status.status_id}">Delete</button>
            </div>
        `;
        
        item.addEventListener('dragstart', handleDragStart);
        item.addEventListener('dragover', handleDragOver);
        item.addEventListener('drop', handleDrop);
        item.addEventListener('dragend', handleDragEnd);
        
        container.appendChild(item);
    });
    
    // Attach event listeners to dynamically created buttons
    attachButtonListeners();
}

function attachButtonListeners() {
    document.querySelectorAll('.btn-edit').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const statusId = parseInt(e.target.dataset.statusId);
            editStatus(statusId);
        });
    });
    
    document.querySelectorAll('.btn-delete').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const statusId = parseInt(e.target.dataset.statusId);
            confirmDeleteStatus(statusId);
        });
    });
}

function handleDragStart(e) {
    draggedElement = e.target;
    e.target.classList.add('dragging');
}

function handleDragOver(e) {
    e.preventDefault();
    const afterElement = getDragAfterElement(document.getElementById('status-list'), e.clientY);
    if (afterElement == null) {
        document.getElementById('status-list').appendChild(draggedElement);
    } else {
        document.getElementById('status-list').insertBefore(draggedElement, afterElement);
    }
}

function handleDrop(e) {
    e.preventDefault();
    updateStatusOrder();
}

function handleDragEnd(e) {
    e.target.classList.remove('dragging');
}

function getDragAfterElement(container, y) {
    const draggableElements = [...container.querySelectorAll('.status-item:not(.dragging)')];
    
    return draggableElements.reduce((closest, child) => {
        const box = child.getBoundingClientRect();
        const offset = y - box.top - box.height / 2;
        
        if (offset < 0 && offset > closest.offset) {
            return { offset: offset, element: child };
        } else {
            return closest;
        }
    }, { offset: Number.NEGATIVE_INFINITY }).element;
}

function updateStatusOrder() {
    const items = document.querySelectorAll('.status-item');
    statuses = Array.from(items).map((item, index) => {
        const statusId = parseInt(item.dataset.statusId);
        const status = statuses.find(s => s.status_id === statusId);
        status.s_order = index;
        return status;
    });
}

async function saveOrder() {
    try {
        const response = await fetch('/api/statuses/reorder', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({statuses: statuses})
        });
        
        if (response.ok) {
            alert('Status order saved successfully!');
        } else {
            alert('Error saving order');
        }
    } catch (error) {
        console.error('Error saving order:', error);
    }
}

function openStatusPopup(statusId = null) {
    editingStatusId = statusId;
    const popup = document.getElementById('status-popup');
    const title = document.getElementById('popup-title');
    const nameInput = document.getElementById('status-name');
    const colorInput = document.getElementById('status-color');
    
    if (statusId) {
        const status = statuses.find(s => s.status_id === statusId);
        title.textContent = 'Edit Status';
        nameInput.value = status.s_status;
        colorInput.value = status.s_color;
    } else {
        title.textContent = 'Add New Status';
        nameInput.value = '';
        colorInput.value = '#808080';
    }
    
    document.getElementById('status-popup-overlay').style.display = 'block';
    popup.style.display = 'block';
}

function editStatus(statusId) {
    console.log('Editing status', statusId);
    openStatusPopup(statusId);
}

async function saveStatus() {
    const nameInput = document.getElementById('status-name');
    const colorInput = document.getElementById('status-color');
    
    const statusData = {
        s_status: nameInput.value.trim(),
        s_color: colorInput.value,
        s_order: editingStatusId ? statuses.find(s => s.status_id === editingStatusId).s_order : statuses.length
    };
    
    if (!statusData.s_status) {
        alert('Status name is required');
        return;
    }
    
    try {
        const url = editingStatusId ? `/api/statuses/${editingStatusId}` : '/api/statuses';
        const method = editingStatusId ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method: method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(statusData)
        });
        
        if (response.ok) {
            closePopup('status-popup');
            loadStatuses();
        } else {
            const error = await response.json();
            alert('Error: ' + error.error);
        }
    } catch (error) {
        console.error('Error saving status:', error);
    }
}

function confirmDeleteStatus(statusId) {
    editingStatusId = statusId;
    document.getElementById('delete-popup-overlay').style.display = 'block';
    document.getElementById('delete-popup').style.display = 'block';
}

async function deleteStatus() {
    try {
        const response = await fetch(`/api/statuses/${editingStatusId}`, {
            method: 'DELETE'
        });
        
        if (response.ok) {
            closePopup('delete-popup');
            loadStatuses();
        } else {
            const error = await response.json();
            alert('Error: ' + error.error);
        }
    } catch (error) {
        console.error('Error deleting status:', error);
    }
}

function closePopup(popupName) {
    if (popupName === 'status-popup') {
        document.getElementById('status-popup-overlay').style.display = 'none';
        document.getElementById('status-popup').style.display = 'none';
    } else if (popupName === 'delete-popup') {
        document.getElementById('delete-popup-overlay').style.display = 'none';
        document.getElementById('delete-popup').style.display = 'none';
    } else {
        // Close all
        document.querySelectorAll('.popup-overlay').forEach(overlay => overlay.style.display = 'none');
        document.querySelectorAll('.popup').forEach(popup => popup.style.display = 'none');
    }
    editingStatusId = null;
}