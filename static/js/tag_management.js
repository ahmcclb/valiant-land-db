let tags = [];
let editingTagId = null;
let deletingTagId = null;

document.addEventListener('DOMContentLoaded', function() {
    loadTags();
    setupEventListeners();
});

function setupEventListeners() {
    document.getElementById('add-tag').addEventListener('click', openAddTagPopup);
    
    // Popup controls
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('close-popup')) {
            closePopup(e.target.dataset.popup);
        }
    });
    
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('btn-cancel') && e.target.dataset.popup) {
            closePopup(e.target.dataset.popup);
        }
    });
    
    // Overlay clicks
    document.getElementById('tag-popup-overlay').addEventListener('click', () => closePopup('tag-popup'));
    document.getElementById('delete-popup-overlay').addEventListener('click', () => closePopup('delete-popup'));
    
    // Action buttons
    document.getElementById('save-tag').addEventListener('click', saveTag);
    document.getElementById('confirm-delete-tag').addEventListener('click', deleteTag);
}

async function loadTags() {
    try {
        const response = await fetch('/api/tags');
        const data = await response.json();
        tags = data.tags;
        renderTagList();
    } catch (error) {
        console.error('Error loading tags:', error);
    }
}

function renderTagList() {
    const container = document.getElementById('tag-list');
    container.innerHTML = '';
    
    if (tags.length === 0) {
        container.innerHTML = '<p style="padding: 20px; color: #666;">No tags found. Click "Add Tag" to create one.</p>';
        return;
    }
    
    tags.forEach(tag => {
        const item = document.createElement('div');
        item.className = 'tag-item';
        item.dataset.tagId = tag.tag_id;
        item.dataset.editing = 'false';
        
        item.innerHTML = `
            <div class="tag-info">
                <span class="tag-id">${tag.tag_id}</span>
                <span class="tag-name-display">${tag.tag_name}</span>
                <input type="text" class="tag-name-edit" value="${tag.tag_name}" style="display: none;">
            </div>
            <div class="tag-controls">
                <button class="btn-edit" data-action="edit">Edit</button>
            </div>
        `;
        
        container.appendChild(item);
    });
    
    attachItemListeners();
}

function attachItemListeners() {
    document.querySelectorAll('.tag-item').forEach(item => {
        const editBtn = item.querySelector('.btn-edit');
        editBtn.addEventListener('click', (e) => {
            const tagId = parseInt(item.dataset.tagId);
            toggleEditMode(tagId);
        });
    });
}

function toggleEditMode(tagId) {
    const item = document.querySelector(`[data-tag-id="${tagId}"]`);
    const isEditing = item.dataset.editing === 'true';
    
    if (isEditing) {
        // Cancel edit mode
        item.dataset.editing = 'false';
        item.querySelector('.tag-name-display').style.display = 'inline';
        item.querySelector('.tag-name-edit').style.display = 'none';
        
        // Reset buttons
        item.querySelector('.tag-controls').innerHTML = '<button class="btn-edit" data-action="edit">Edit</button>';
        attachItemListeners();
    } else {
        // Enter edit mode
        item.dataset.editing = 'true';
        item.querySelector('.tag-name-display').style.display = 'none';
        item.querySelector('.tag-name-edit').style.display = 'inline';
        
        // Replace Edit button with Save/Delete/Cancel
        item.querySelector('.tag-controls').innerHTML = `
            <button class="btn-save" data-action="save">Save</button>
            <button class="btn-delete" data-action="delete">Delete</button>
            <button class="btn-cancel" data-action="cancel">Cancel</button>
        `;
        
        // Attach listeners for the new buttons
        item.querySelector('[data-action="save"]').addEventListener('click', () => saveTagEdit(tagId));
        item.querySelector('[data-action="delete"]').addEventListener('click', () => confirmDeleteTag(tagId));
        item.querySelector('[data-action="cancel"]').addEventListener('click', () => toggleEditMode(tagId));
    }
}

async function saveTagEdit(tagId) {
    const item = document.querySelector(`[data-tag-id="${tagId}"]`);
    const newName = item.querySelector('.tag-name-edit').value.trim();
    
    if (!newName) {
        alert('Tag name cannot be empty');
        return;
    }
    
    try {
        const response = await fetch(`/api/tags/${tagId}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tag_name: newName})
        });
        
        if (response.ok) {
            // Update local data and re-render
            const tag = tags.find(t => t.tag_id === tagId);
            tag.tag_name = newName;
            renderTagList();
        } else {
            alert('Error saving tag');
        }
    } catch (error) {
        console.error('Error saving tag:', error);
    }
}

function openAddTagPopup() {
    document.getElementById('tag-name-input').value = '';
    document.getElementById('tag-popup-overlay').style.display = 'block';
    document.getElementById('tag-popup').style.display = 'block';
}

async function saveTag() {
    const tagName = document.getElementById('tag-name-input').value.trim();
    
    if (!tagName) {
        alert('Tag name is required');
        return;
    }
    
    // Check for duplicates
    const exists = tags.some(t => t.tag_name.toLowerCase() === tagName.toLowerCase());
    if (exists) {
        alert('A tag with this name already exists');
        return;
    }
    
    try {
        const response = await fetch('/api/tags', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({tag_name: tagName})
        });
        
        if (response.ok) {
            closePopup('tag-popup');
            loadTags(); // Reload to show new tag
        } else {
            const error = await response.json();
            alert('Error: ' + error.error);
        }
    } catch (error) {
        console.error('Error creating tag:', error);
    }
}

function confirmDeleteTag(tagId) {
    deletingTagId = tagId;
    document.getElementById('delete-popup-overlay').style.display = 'block';
    document.getElementById('delete-popup').style.display = 'block';
}

async function deleteTag() {
    try {
        const response = await fetch(`/api/tags/${deletingTagId}`, {
            method: 'DELETE'
        });
        
        if (response.ok) {
            closePopup('delete-popup');
            loadTags();
        } else {
            const error = await response.json();
            alert('Error: ' + error.error);
        }
    } catch (error) {
        console.error('Error deleting tag:', error);
    }
}

function closePopup(popupName) {
    if (popupName === 'tag-popup') {
        document.getElementById('tag-popup-overlay').style.display = 'none';
        document.getElementById('tag-popup').style.display = 'none';
    } else if (popupName === 'delete-popup') {
        document.getElementById('delete-popup-overlay').style.display = 'none';
        document.getElementById('delete-popup').style.display = 'none';
    }
    deletingTagId = null;
}