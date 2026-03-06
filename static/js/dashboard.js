// COMPLETE DASHBOARD.JS - PASTE THIS ENTIRELY
// =============================================================================

// Global state
let currentFilter = 'all_in_process';
let currentPage = 1;
let perPage = 10;
let selectedRecords = new Set();
let visibleColumns = ['p_id', 'p_status', 'p_apn', 'or_name', 'p_county', 'p_state', 'p_acres', 'p_comp_market_value'];
let allColumns = [];
let totalPages = 1;
let sortBy = 'p_id';
let sortDir = 'asc';
let availableTags = [];
let availableStatuses = [];

// Initialize on page load
document.addEventListener('DOMContentLoaded', async function() {
    console.log('=== Dashboard initializing ===');
    
    // Load persisted dashboard state
    loadDashboardState();
    
    await loadAvailableColumns();
    setupEventListeners();
    initAdvancedSearch();
    
    // CRITICAL FIX: Check for simple search UI persistence
    const simpleUI = localStorage.getItem('simpleSearchUI');
    if (simpleUI && isSearchActive) {
        // Restore the simple search UI fields
        const ui = JSON.parse(simpleUI);
        document.getElementById('simpleSearchField').value = ui.field;
        updateSimpleSearchInput();
        
        // Restore values based on field type
        if (ui.fieldType === 'date_range') {
            if (ui.value.from && document.getElementById('simpleSearchValueFrom')) {
                document.getElementById('simpleSearchValueFrom').value = ui.value.from;
            }
            if (ui.value.to && document.getElementById('simpleSearchValueTo')) {
                document.getElementById('simpleSearchValueTo').value = ui.value.to;
            }
        } else if (ui.fieldType !== 'special') {
            const valInput = document.getElementById('simpleSearchValue');
            if (valInput && ui.value) {
                valInput.value = ui.value;
            }
        }
        
        // Set button to active state since we're in search mode
        setClearSearchButtonActive(true);
        
        // Load the search results (don't execute search again, just load results)
        loadSearchResults();
    } else if (isSearchActive) {
        // Advanced search persistence
        setClearSearchButtonActive(true);
        loadSearchResults();
    } else {
        loadProperties();
    }
});

// Helper to handle downloads in desktop app vs browser
async function downloadFile(filename) {
    console.log('[DEBUG] downloadFile called for:', filename);
    
    // Check if running in PyWebView desktop app
    if (window.pywebview && window.pywebview.api) {
        try {
            console.log('[DEBUG] Calling Python API download_file');
            const result = await window.pywebview.api.download_file(filename);
            console.log('[DEBUG] API result:', result);
            
            if (result.success) {
                console.log('File saved to:', result.path);
                if (typeof showPopup === 'function') {
                    showPopup('File saved successfully to: ' + result.path);
                } else {
                    alert('File saved to: ' + result.path);
                }
            } else if (result.cancelled) {
                console.log('User cancelled download');
            } else {
                console.error('Download error:', result.error);
                alert('Download error: ' + (result.error || 'Unknown error'));
            }
        } catch (err) {
            console.error('API error:', err);
            alert('Desktop API error: ' + err.message);
            // Fallback to browser behavior
            window.open('/static/exports/' + filename, '_blank');
        }
    } else {
        console.log('[DEBUG] Browser mode - opening file in new tab');
        // Normal browser mode - use standard download
        window.open('/static/exports/' + filename, '_blank');
    }
}

// Setup all event listeners - CALLED ONLY ONCE
function setupEventListeners() {
    console.log('=== Setting up event listeners ===');
    
    // Action menu
    document.querySelectorAll('.menu-btn').forEach(btn => {
        btn.addEventListener('click', handleMenuAction);
    });
    
    // Filter menu
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', handleFilterChange);
    });
    
    // Selection controls
    document.getElementById('select-all-set').addEventListener('click', selectAllInSet);
    
    // THE CRITICAL FIX - Remove any existing listener before adding
    const changeStatusBtn = document.getElementById('change-status');
    if (changeStatusBtn) {
        console.log('Binding change-status button');
        changeStatusBtn.removeEventListener('click', openChangeStatusPopup);
        changeStatusBtn.addEventListener('click', openChangeStatusPopup);
    }
    
    document.getElementById('customize-columns').addEventListener('click', openCustomizePopup);
    
    // Pagination
    document.getElementById('first-page').addEventListener('click', () => goToPage(1));
    document.getElementById('prev-page').addEventListener('click', () => goToPage(currentPage - 1));
    document.getElementById('next-page').addEventListener('click', () => goToPage(currentPage + 1));
    document.getElementById('last-page').addEventListener('click', () => goToPage(totalPages));
    document.getElementById('current-page').addEventListener('change', (e) => goToPage(parseInt(e.target.value)));
    document.getElementById('per-page').addEventListener('change', (e) => {
        perPage = parseInt(e.target.value);
        currentPage = 1;
        loadProperties();
    });
    
    // Popup controls
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('close-popup')) {
            closePopup();
        }
    });
    
    // Overlay click
    const overlay = document.getElementById('popup-overlay');
    if (overlay) {
        overlay.addEventListener('click', closePopup);
    }
    
    // Specific popup buttons
    const customizeCancel = document.querySelector('#customize-popup .btn-cancel');
    if (customizeCancel) customizeCancel.addEventListener('click', closePopup);
    
    const customizeSave = document.querySelector('#customize-popup .btn-save');
    if (customizeSave) customizeSave.addEventListener('click', saveColumnCustomization);
    
    // New Offer popup
    const apnSearch = document.getElementById('apn-search');
    if (apnSearch) apnSearch.addEventListener('input', debounce(searchAPN, 300));
    
    // Delete popup
    const deleteCancel = document.querySelector('#delete-popup .btn-cancel');
    if (deleteCancel) deleteCancel.addEventListener('click', closePopup);
    
    const deleteConfirm = document.querySelector('#delete-popup .btn-confirm-delete');
    if (deleteConfirm) deleteConfirm.addEventListener('click', confirmDelete);
}

async function loadSearchOptions() {
    try {
        // Load tags
        const tagsResponse = await fetch('/api/tags');
        const tagsData = await tagsResponse.json();
        availableTags = tagsData.tags || [];
        
        // Load statuses
        const statusResponse = await fetch('/api/statuses');
        const statusData = await statusResponse.json();
        availableStatuses = statusData.statuses || [];
    } catch (error) {
        console.error('Error loading search options:', error);
    }
}

// Load available columns
async function loadAvailableColumns() {
    try {
        const response = await fetch('/api/dashboard/columns');
        const data = await response.json();
        allColumns = data.columns;
        
        const savedColumns = localStorage.getItem('dashboard_columns');
        if (savedColumns) {
            visibleColumns = JSON.parse(savedColumns);
        } else {
            visibleColumns = allColumns
                .filter(col => col.default_visible)
                .map(col => col.field);
        }
    } catch (error) {
        console.error('Error loading columns:', error);
    }
}

// Handle menu actions
function handleMenuAction(event) {
    const action = event.target.dataset.action;
    
    switch(action) {
        case 'new-offer-request':
            openNewOfferPopup();
            break;
        case 'add-record':
            window.location.href = '/property/new';
            break;
        case 'import':
            window.location.href = '/import';
            break;
        case 'export-all':
            exportAllData();
            break;
        case 'mailing':
            openMailingPopup();
            break;
        case 'generate-docs':
            // NEW: Open document creation page with selected properties
            if (selectedRecords.size === 0) {
                alert('No records selected for document generation');
                return;
            }
            const propertyIds = Array.from(selectedRecords).join(',');
            window.location.href = `/documents?properties=${propertyIds}`;
            break;
        case 'status':
            window.location.href = '/status-management';
            break;
        case 'tags':
            window.location.href = '/tags';
            break;
        case 'company-info':
            window.location.href = '/company';
            break;
        case 'advanced-search':
            alert('Advanced Search popup will open (spec needed)');
            break;
    }
}

function handleFilterChange(event) {
    // CRITICAL FIX: Reset search state when clicking filter buttons
    isSearchActive = false;
    localStorage.removeItem('advancedSearchState');
    localStorage.removeItem('simpleSearchUI');
    setClearSearchButtonActive(false);  // Reset button color
    
    // Remove search indicator if present
    const indicator = document.getElementById('search-active-indicator');
    if (indicator) indicator.remove();

    // Update UI
    document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.remove('active'));
    event.target.classList.add('active');
    
    // Update state
    currentFilter = event.target.dataset.filter;
    currentPage = 1;
    selectedRecords.clear();
    updateSelectedCount();
    
    // Load properties with filter (not search)
    loadProperties();
}

// Load properties
async function loadProperties() {
    try {
        const columnsParam = visibleColumns.join(',');
        const url = `/api/dashboard/properties?filter=${currentFilter}&page=${currentPage}&per_page=${perPage}&columns=${columnsParam}&sort_by=${sortBy}&sort_dir=${sortDir}`;
        
        console.log('Loading properties:', url);
        const response = await fetch(url);
        const data = await response.json();
        
        renderTable(data.properties);
        updatePagination(data.pagination);
        totalPages = data.pagination.total_pages;
        
        const allVisibleSelected = data.properties.every(row => selectedRecords.has(row.p_id));
		const headerCheckbox = document.getElementById('header-checkbox');
		if (headerCheckbox) {
			headerCheckbox.checked = allVisibleSelected;
		}
				
			} catch (error) {
				console.error('Error loading properties:', error);
			}
}

// Render table
function renderTable(properties) {
    const header = document.getElementById('table-header');
    const body = document.getElementById('table-body');
    
    header.innerHTML = '';
    body.innerHTML = '';
    
    const checkboxTh = document.createElement('th');
    checkboxTh.innerHTML = '<input type="checkbox" id="header-checkbox">';
    header.appendChild(checkboxTh);
    
    visibleColumns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = getColumnLabel(col);
        th.dataset.field = col;
        th.addEventListener('click', () => sortTable(col));
        header.appendChild(th);
    });
    
    const editTh = document.createElement('th');
    editTh.textContent = 'Edit';
    header.appendChild(editTh);
    
    const deleteTh = document.createElement('th');
    const bulkDeleteBtn = document.createElement('button');
    bulkDeleteBtn.textContent = 'Delete';
    bulkDeleteBtn.className = 'bulk-delete-btn';
    bulkDeleteBtn.style.cssText = 'background: #dc3545; color: white; border: none; padding: 5px 10px; border-radius: 4px; cursor: pointer; font-size: 12px;';
    bulkDeleteBtn.onclick = confirmBulkDelete;
    deleteTh.appendChild(bulkDeleteBtn);
    header.appendChild(deleteTh);
    
    properties.forEach(property => {
        const row = document.createElement('tr');
        
        const checkboxTd = document.createElement('td');
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.className = 'row-checkbox';
        checkbox.dataset.pId = property.p_id;
        checkbox.checked = selectedRecords.has(property.p_id);
        checkbox.addEventListener('change', () => toggleRowSelection(property.p_id));
        checkboxTd.appendChild(checkbox);
        row.appendChild(checkboxTd);
        
        visibleColumns.forEach(col => {
            const td = document.createElement('td');
            
            if (col === 'p_status') {
                td.textContent = property.p_status || '';
            } else if (col === 'tags') {
                td.textContent = property.tags || '';
            } else {
                td.textContent = property[col] || '';
            }
            
            row.appendChild(td);
        });
        
        const editTd = document.createElement('td');
        const editBtn = document.createElement('button');
        editBtn.className = 'icon-btn';
        editBtn.innerHTML = '✏️';
        editBtn.title = 'Edit Record';
        editBtn.addEventListener('click', () => editProperty(property.p_id));
        editTd.appendChild(editBtn);
        row.appendChild(editTd);
        
        const deleteTd = document.createElement('td');
        const deleteBtn = document.createElement('button');
        deleteBtn.className = 'icon-btn';
        deleteBtn.innerHTML = '🗑️';
        deleteBtn.title = 'Delete Record';
        deleteBtn.addEventListener('click', () => confirmDeleteProperty(property.p_id));
        deleteTd.appendChild(deleteBtn);
        row.appendChild(deleteTd);
        
        body.appendChild(row);
    });
    
    document.getElementById('header-checkbox').addEventListener('change', toggleSelectAllVisible);
}

// Get column label
function getColumnLabel(field) {
    const column = allColumns.find(col => col.field === field);
    return column ? column.label : field;
}

// Toggle row selection
function toggleRowSelection(pId) {
    if (selectedRecords.has(pId)) {
        selectedRecords.delete(pId);
    } else {
        selectedRecords.add(pId);
    }
    updateSelectedCount();
}

// Toggle select all visible
function toggleSelectAllVisible(event) {
    const checkboxes = document.querySelectorAll('.row-checkbox');
    checkboxes.forEach(checkbox => {
        const pId = parseInt(checkbox.dataset.pId);
        if (event.target.checked) {
            selectedRecords.add(pId);
            checkbox.checked = true;
        } else {
            selectedRecords.delete(pId);
            checkbox.checked = false;
        }
    });
    updateSelectedCount();
}

// Select all in set
function selectAllInSet() {
    // Check current state - if we already have all selected, deselect all
    fetch(`/api/dashboard/properties?filter=${currentFilter}&page=1&per_page=1&columns=p_id`)
        .then(response => response.json())
        .then(countData => {
            const totalInSet = countData.pagination.total_records;
            
            // If we already have all records selected, deselect all (toggle off)
            if (selectedRecords.size === totalInSet && totalInSet > 0) {
                selectedRecords.clear();
                document.querySelectorAll('.row-checkbox').forEach(checkbox => {
                    checkbox.checked = false;
                });
                document.getElementById('header-checkbox').checked = false;
                updateSelectedCount();
                console.log('Deselected all records');
                return;
            }
            
            // Otherwise select all (toggle on)
            return fetch(`/api/dashboard/properties?filter=${currentFilter}&page=1&per_page=${totalInSet}&columns=p_id`)
                .then(response => response.json())
                .then(data => {
                    selectedRecords.clear();
                    data.properties.forEach(property => {
                        selectedRecords.add(property.p_id);
                    });
                    
                    document.querySelectorAll('.row-checkbox').forEach(checkbox => {
                        checkbox.checked = true;
                    });
                    
                    document.getElementById('header-checkbox').checked = true;
                    updateSelectedCount();
                    console.log(`Selected ${selectedRecords.size} records total`);
                });
        })
        .catch(error => console.error('Error in selectAllInSet:', error));
}

// Update selected count
function updateSelectedCount() {
    const totalSelected = selectedRecords.size;
    const visibleCount = document.querySelectorAll('.row-checkbox').length;
    const selectedVisible = Array.from(document.querySelectorAll('.row-checkbox'))
                                .filter(cb => cb.checked).length;
    
    // Show "27 (10 visible)" when more selected than visible
    let displayText = totalSelected.toString();
    if (totalSelected > selectedVisible) {
        displayText += ` (${selectedVisible} visible)`;
    }
    
    document.getElementById('selected-count').textContent = displayText;
}

// Sort table
function sortTable(field) {
    // Remove sorting indicators
    document.querySelectorAll('#table-header th').forEach(th => {
        th.classList.remove('sorted-asc', 'sorted-desc');
    });
    
    if (sortBy === field) {
        sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    } else {
        sortBy = field;
        sortDir = 'asc';
    }
    
    // Add indicator to current sort column
    const headerTh = document.querySelector(`#table-header th[data-field="${field}"]`);
    if (headerTh) {
        headerTh.classList.add(`sorted-${sortDir}`);
    }
    
    currentPage = 1; // Reset to first page when sorting
    loadProperties();
}

// Update pagination
function updatePagination(pagination) {
    document.getElementById('current-page').value = pagination.current_page;
    document.getElementById('total-pages').textContent = pagination.total_pages;
    document.getElementById('per-page').value = perPage;
    
    document.getElementById('first-page').disabled = pagination.current_page === 1;
    document.getElementById('prev-page').disabled = pagination.current_page === 1;
    document.getElementById('next-page').disabled = pagination.current_page === pagination.total_pages;
    document.getElementById('last-page').disabled = pagination.current_page === pagination.total_pages;
}

// Go to page
function goToPage(page) {
    if (page < 1) page = 1;
    if (page > totalPages) page = totalPages;
    
    currentPage = page;
    loadProperties();
}

// Edit property
function editProperty(pId) {
    window.location.href = `/property/edit/${pId}`;
}

// Delete confirmation
let propertyToDelete = null;

function confirmDeleteProperty(pId) {
    propertyToDelete = pId;
    openPopup('delete-popup');
}

async function confirmDelete() {
    if (!propertyToDelete) return;
    
    try {
        const response = await fetch(`/api/property/${propertyToDelete}`, {
            method: 'DELETE'
        });
        
        if (response.ok) {
            selectedRecords.delete(propertyToDelete);
            updateSelectedCount();
            loadProperties();
            closePopup();
        } else {
            alert('Error deleting record');
        }
    } catch (error) {
        console.error('Error deleting:', error);
    }
}

// Export all data
async function exportAllData() {
    if (selectedRecords.size === 0) {
        alert('No records selected for export');
        return;
    }
    
    try {
        const columns = allColumns.map(col => col.field).join(',');
        const url = `/api/dashboard/properties?filter=${currentFilter}&page=1&per_page=${selectedRecords.size}&columns=${columns}`;
        console.log('[DEBUG] Fetching data for export:', url);
        const response = await fetch(url);
        const data = await response.json();
        
        console.log('[DEBUG] Sending export request to server');
        const exportResponse = await fetch('/api/dashboard/export/csv', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({records: data.properties})
        });
        
        if (!exportResponse.ok) {
            const errorData = await exportResponse.json();
            throw new Error(errorData.error || 'Export failed');
        }
        
        // Get blob from response
        const blob = await exportResponse.blob();
        console.log('[DEBUG] Received blob:', blob.size, 'bytes');
        
        // Get filename from header or generate default
        const disposition = exportResponse.headers.get('content-disposition');
        let filename = 'Export.csv';
        if (disposition && disposition.includes('filename=')) {
            filename = disposition.split('filename=')[1].replace(/["']/g, '');
        }
        console.log('[DEBUG] Filename:', filename);
        
        // Desktop app vs browser handling - FIXED to use readAsDataURL instead of manual base64
        if (window.pywebview && window.pywebview.api) {
            console.log('[DEBUG] Desktop mode - converting blob to data URL');
            const reader = new FileReader();
            reader.onload = async function() {
                try {
                    const dataUrl = reader.result; // This is data:text/csv;base64,...
                    console.log('[DEBUG] Data URL length:', dataUrl.length);
                    console.log('[DEBUG] Calling save_download_file');
                    const result = await window.pywebview.api.save_download_file(filename, dataUrl);
                    console.log('[DEBUG] save_download_file result:', result);
                    
                    if (result.success) {
                        alert('Export complete! File saved to: ' + result.path);
                    } else if (!result.cancelled) {
                        alert('Save failed: ' + (result.error || 'Unknown error'));
                    }
                } catch (err) {
                    console.error('Error in save_download_file call:', err);
                    alert('Error saving file: ' + err.message);
                }
            };
            reader.onerror = function(err) {
                console.error('FileReader error:', err);
                alert('Error reading file data');
            };
            reader.readAsDataURL(blob);
        } else {
            // Browser mode - use standard download
            const downloadUrl = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = downloadUrl;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(downloadUrl);
        }
        
    } catch (error) {
        console.error('Export error:', error);
        alert('Export failed: ' + error.message);
    }
}


// Mailing export
function openMailingPopup() {
    if (selectedRecords.size === 0) {
        alert('No records selected for mailing export');
        return;
    }
    
    // Show the mailing type selection popup
    document.getElementById('popup-overlay').style.display = 'block';
    document.getElementById('mailing-type-popup').style.display = 'block';
    
    // Add event listeners to the buttons
    document.getElementById('export-usmail-btn').onclick = () => {
        closePopup();
        exportMailingList('usmail');
    };
    
    document.getElementById('export-email-btn').onclick = () => {
        closePopup();
        exportMailingList('email');
    };
}

async function exportMailingList(type) {
    try {
        const columns = ['p_id', 'or_id', 'p_status', 'p_state', 'p_county', 'p_apn', 
                        'p_longstate', 'or_fname', 'or_lname', 'or_email',
                        'o_type', 'o_company', 'or_m_address', 'or_m_city', 
                        'or_m_state', 'or_m_zip'].join(',');
        
        const url = `/api/dashboard/properties?filter=all_in_process&page=1&per_page=1000&columns=${columns}`;
        const response = await fetch(url);
        const data = await response.json();
        
        // CRITICAL FIX: Check if data.properties exists
        if (!data.properties) {
            throw new Error('No properties data received from server');
        }
        
        const records = data.properties.filter(p => selectedRecords.has(p.p_id));
        
        if (records.length === 0) {
            alert('No records selected for export');
            return;
        }
        
        const exportResponse = await fetch('/api/dashboard/export/mailing', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({records: records, type: type})
        });
        
        if (!exportResponse.ok) {
            const errorData = await exportResponse.json();
            throw new Error(errorData.error || 'Export failed');
        }
        
        const blob = await exportResponse.blob();
        
        const disposition = exportResponse.headers.get('content-disposition');
        let filename = type === 'usmail' ? 'Mailing.csv' : 'Emailing.csv';
        if (disposition && disposition.includes('filename=')) {
            filename = disposition.split('filename=')[1].replace(/["']/g, '');
        }
        
        // CRITICAL FIX: Use readAsDataURL pattern
        if (window.pywebview && window.pywebview.api) {
            const reader = new FileReader();
            reader.onload = async function() {
                try {
                    const dataUrl = reader.result;
                    const result = await window.pywebview.api.save_download_file(filename, dataUrl);
                    
                    if (result.success) {
                        console.log('File saved to:', result.path);
						alert('Export complete! File saved to: ' + result.path); 
                    } else if (result.cancelled) {
                        console.log('User cancelled');
                    } else {
                        alert('Save error: ' + (result.error || 'Unknown'));
                    }
                } catch (err) {
                    console.error('API error:', err);
                    alert('Error: ' + err.message);
                }
            };
            reader.onerror = function() {
                alert('Error reading file data');
            };
            reader.readAsDataURL(blob);
        } else {
            const downloadUrl = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = downloadUrl;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(downloadUrl);
        }
        
    } catch (error) {
        console.error('Mailing export error:', error);
        alert('Export failed: ' + error.message);
    }
}

// Column customization
function openCustomizePopup() {
    const popup = document.getElementById('customize-popup');
    const columnList = document.getElementById('column-list');
    
    columnList.innerHTML = '';
    
    allColumns.forEach(col => {
        const item = document.createElement('div');
        item.className = 'column-item';
        
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = `col-${col.field}`;
        checkbox.checked = visibleColumns.includes(col.field);
        
        const label = document.createElement('label');
        label.htmlFor = `col-${col.field}`;
        label.textContent = col.label;
        
        item.appendChild(checkbox);
        item.appendChild(label);
        columnList.appendChild(item);
    });
    
    openPopup('customize-popup');
}

function saveColumnCustomization() {
    const checkboxes = document.querySelectorAll('#column-list input[type="checkbox"]');
    visibleColumns = [];
    
    checkboxes.forEach(checkbox => {
        if (checkbox.checked) {
            const field = checkbox.id.replace('col-', '');
            visibleColumns.push(field);
        }
    });
    
    localStorage.setItem('dashboard_columns', JSON.stringify(visibleColumns));
    
    closePopup();
    loadProperties();
}

// New Offer Request popup
function openNewOfferPopup() {
    document.getElementById('apn-search').value = '';
    document.getElementById('apn-results').innerHTML = '';
    openPopup('new-offer-popup');
}

async function searchAPN() {
    const apn = document.getElementById('apn-search').value.trim();
    const resultsDiv = document.getElementById('apn-results');
    
    if (apn.length < 2) {
        resultsDiv.innerHTML = '';
        return;
    }
    
    try {
        // Search for APN only - no county filter
        const url = `/api/dashboard/properties?search=${encodeURIComponent(apn)}&columns=p_id,p_apn,or_fname,or_lname,o_company,p_county,p_state,p_status`;
        const response = await fetch(url);
        const data = await response.json();
        
        resultsDiv.innerHTML = '';
        
        if (data.properties.length === 0) {
            resultsDiv.innerHTML = '<div style="color: #dc3545; font-weight: bold; margin-bottom: 15px;">APN not found</div>';
            const newBtn = document.createElement('button');
            newBtn.textContent = 'Create New Record';
            newBtn.className = 'btn-primary';
            newBtn.style.marginTop = '10px';
            newBtn.addEventListener('click', () => {
                const params = new URLSearchParams();
                params.append('apn', apn);
                // Only passing APN, no county
                window.location.href = `/new-offer-request?${params.toString()}`;
            });
            resultsDiv.appendChild(newBtn);
        } else {
            data.properties.forEach(prop => {
                const row = document.createElement('div');
                row.className = 'apn-result-row';
                
                const name = prop.o_type === 'Company' 
                    ? prop.o_company 
                    : `${prop.or_fname || ''} ${prop.or_lname || ''}`.trim();
                
                row.innerHTML = `
                    <strong>APN:</strong> ${prop.p_apn}<br>
                    <strong>County:</strong> ${prop.p_county}<br>
                    <strong>State:</strong> ${prop.p_state}<br>
                    <strong>Owner:</strong> ${name}<br>
                    <strong>Status:</strong> ${prop.p_status || ''}
                    <button class="select-apn-btn" data-pid="${prop.p_id}">Choose this Property</button>
                `;
                
                row.querySelector('.select-apn-btn').addEventListener('click', () => {
                    window.location.href = `/new-offer-request?p_id=${prop.p_id}`;
                });
                
                resultsDiv.appendChild(row);
            });
        }
    } catch (error) {
        console.error('APN search error:', error);
        resultsDiv.innerHTML = '<div style="color: red;">Error searching properties: ' + error.message + '</div>';
    }
}

// Change Status Popup
async function openChangeStatusPopup() {
    console.log('=== openChangeStatusPopup CALLED ===');
    console.trace(); // Show who called this function
    
    if (selectedRecords.size === 0) {
        alert('No records selected for status change');
        return;
    }
    
    const popup = document.getElementById('change-status-popup');
    const dropdown = document.getElementById('status-dropdown');
    const recordCount = document.getElementById('status-record-count');
    
    dropdown.innerHTML = '<option value="">-- Select Status --</option>';
    
    try {
        const response = await fetch('/api/statuses');
        const data = await response.json();
        
        data.statuses.forEach(status => {
            const option = document.createElement('option');
            option.value = status.status_id;
            option.textContent = status.s_status;
            dropdown.appendChild(option);
        });
        
        recordCount.textContent = selectedRecords.size;
        
        document.querySelector('.popup-overlay').style.display = 'block';
        popup.style.display = 'block';
        
        document.getElementById('confirm-status-change').onclick = async function() {
            const newStatusId = dropdown.value;
            if (!newStatusId) {
                alert('Please select a status');
                return;
            }
            
            await changeStatusForRecords(Array.from(selectedRecords), parseInt(newStatusId));
        };
        
    } catch (error) {
        console.error('Error loading statuses:', error);
    }
}

async function changeStatusForRecords(pIds, newStatusId) {
    try {
        const response = await fetch('/api/properties/change-status', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                p_ids: pIds,
                status_id: newStatusId
            })
        });
        
        if (response.ok) {
            alert(`Status updated for ${pIds.length} records!`);
            closePopup();
            selectedRecords.clear();
            updateSelectedCount();
            loadProperties();
        } else {
            alert('Error updating status');
        }
    } catch (error) {
        console.error('Error changing status:', error);
    }
}

// Utility functions
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

function openPopup(popupId) {
    document.getElementById('popup-overlay').style.display = 'block';
    document.getElementById(popupId).style.display = 'block';
}

function closePopup() {
    document.getElementById('popup-overlay').style.display = 'none';
    document.querySelectorAll('.popup').forEach(popup => {
        popup.style.display = 'none';
    });
}
// Bulk Delete
function confirmBulkDelete() {
    if (selectedRecords.size === 0) {
        alert('No records selected for deletion');
        return;
    }
    
    if (confirm(`Are you sure you want to delete ${selectedRecords.size} records? This action cannot be undone.`)) {
        executeBulkDelete();
    }
}

async function executeBulkDelete() {
    try {
        const idsToDelete = Array.from(selectedRecords);
        console.log('Attempting to delete IDs:', idsToDelete);
        
        const response = await fetch('/api/properties/bulk-delete', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({p_ids: idsToDelete})
        });
        
        console.log('Response status:', response.status);
        
        const data = await response.json();
        console.log('Response data:', data);
        
        if (response.ok) {
            alert(data.message);
            selectedRecords.clear();
            updateSelectedCount();
            loadProperties();
        } else {
            alert('Error from server: ' + (data.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Full error details:', error);
        alert('Failed to delete records. Check browser console (F12) for details.');
    }
}

// =============================================================================
// ADVANCED SEARCH FUNCTIONALITY - Round 13
// =============================================================================

let searchRules = [];
let searchLogic = 'AND';
let isSearchActive = false;

// Field definitions with types
const searchFieldTypes = {
    // Numbers
    'p_base_tax': 'number', 'p_back_tax': 'number', 'p_betty_score': 'number',
    'p_comp_market_value': 'number', 'p_county_assessed_value': 'number',
    'p_county_market_value': 'number', 'p_hoa': 'number', 'p_impact_fee': 'number',
    'p_liens': 'number', 'p_max_offer_amount': 'number', 'p_min_acceptable_offer': 'number',
    'p_price': 'number', 'p_est_value': 'number', 'p_acres': 'number', 'p_sqft': 'number',
    'p_purchase_amount': 'number', 'p_purchase_closing_costs': 'number',
    'p_sold_amount': 'number', 'p_sold_closing_costs': 'number', 'p_sale_price': 'number',
    'p_id': 'number', 'or_id': 'number', 'p_owned': 'number',
    
	// Dropdowns
	'tag_ids': 'dropdown',
	'tags': 'dropdown', 
	'p_status': 'dropdown',
	'p_aquired': 'dropdown',
	'p_terrain': 'dropdown',
	'p_access': 'dropdown',
	'p_power': 'dropdown',
	'p_state': 'dropdown',
	'or_m_state': 'dropdown',
	
    // Dates
    'p_contract_expires_date': 'date', 'p_create_time': 'date',
    'p_offer_accept_date': 'date', 'p_purchased_on': 'date', 'p_sold_on': 'date',
    'p_last_sold_date': 'date', 'p_last_transaction_date': 'date',
    'p_status_last_updated': 'date', 'p_m_date': 'date', 'p_last_updated': 'date',
    
    // Booleans
    'p_viable': 'boolean', 'p_listed': 'boolean', 'p_survey': 'boolean',
    
    // Text (default)
    'o_2fname': 'text', 'o_2lname': 'text', 'p_apn': 'text', 'p_comments': 'text',
    'o_company': 'text', 'p_county': 'text', 'p_environmental': 'text',
    'or_fname': 'text', 'p_flood_description': 'text', 'or_lname': 'text',
    'or_m_address': 'text', 'or_m_address2': 'text', 'or_m_city': 'text',
    'or_m_state': 'text', 'or_m_zip': 'text', 'p_note': 'text', 'o_fname': 'text',
    'o_lname': 'text', 'o_type': 'text', 'or_phone': 'text', 'p_address': 'text',
    'p_city': 'text', 'p_state': 'text', 'p_zip': 'text',
    'p_closing_company_name_purchase': 'text', 'p_closing_company_name_sale': 'text',
    'p_agent_name': 'text', 'p_agent_phone': 'text', 'p_restrictions': 'text',
    'p_short_legal': 'text', 'p_status': 'text', 'p_terrain': 'text',
    'p_aquired': 'text', 'p_use': 'text', 'p_use_code': 'text', 'p_use_description': 'text',
    'p_zoning': 'text', 'or_email': 'text', 'p_waste_system_requirement': 'text',
    'p_water_system_requirement': 'text', 'p_flood': 'text', 'p_power': 'text',
    'p_access': 'text', 'p_improvements': 'text'
};

const fieldOptions = [
    { value: '-1', label: '------' },
    { value: 'all_fields', label: 'Keyword' },
    { value: 'p_id', label: 'Property Id' },
    { value: 'p_apn', label: 'APN' },
    { value: 'p_status', label: 'Status' },
    { value: 'o_type', label: 'Owner Type' },
    { value: 'or_fname', label: 'Caller First Name' },
    { value: 'or_lname', label: 'Caller Last Name' },
    { value: 'o_fname', label: 'Owner First Name' },
    { value: 'o_lname', label: 'Owner Last Name' },
	{ value: 'o_company', label: 'Company Name' },
    { value: 'or_email', label: 'Email' },
    { value: 'or_phone', label: 'Phone (Primary)' },
    { value: 'p_county', label: 'Property County' },
    { value: 'p_state', label: 'Property State' },	
    { value: 'or_m_address', label: 'Mailing Address' },
    { value: 'or_m_city', label: 'Mailing City' },
    { value: 'or_m_state', label: 'Mailing State' },
    { value: 'o_2fname', label: '2nd Owner\'s First Name' },
    { value: 'o_2lname', label: '2nd Owner\'s Last Name' },
    { value: 'p_aquired', label: 'Acquisition Type' },
    { value: 'p_base_tax', label: 'Annual Base Taxes' },
    { value: 'p_back_tax', label: 'Back Taxes' },
	{ value: 'p_betty_score', label: 'Betty Score' },
    { value: 'p_comments', label: 'Callers Comments' },
    { value: 'p_comp_market_value', label: 'Comped Market Value' },
    { value: 'p_county_assessed_value', label: 'County Assessed Value' },	
    { value: 'p_county_market_value', label: 'County Market Value' },
    { value: 'p_create_time', label: 'Date Created' },
    { value: 'p_environmental', label: 'Environmental Concerns' },
    { value: 'p_flood', label: 'Flood Zone' },
    { value: 'p_flood_description', label: 'Flood Zone Description' },
    { value: 'p_hoa', label: 'HOA Fee' },
    { value: 'p_impact_fee', label: 'Impact Fee' },
    { value: 'p_improvements', label: 'Improvements' },
    { value: 'p_last_sold_date', label: 'Last Sold Date' },
    { value: 'p_last_transaction_date', label: 'Last Transaction Date' },
    { value: 'p_liens', label: 'Liens' },
    { value: 'p_listed', label: 'Listed' },
    { value: 'or_m_address2', label: 'Mailing Address 2' },
    { value: 'or_m_zip', label: 'Mailing Zip' },
    { value: 'p_max_offer_amount', label: 'Maximum Offer Amount' },
    { value: 'p_min_acceptable_offer', label: 'Min Acceptable Offer' },
    { value: 'p_note', label: 'Notes' },
    { value: 'p_offer_accept_date', label: 'Offer Accept By' },
    { value: 'p_price', label: 'Offer Amount' },
    { value: 'p_contract_expires_date', label: 'Offer Expires On' },
    { value: 'p_m_date', label: 'Offer Mail Date' },
    { value: 'p_est_value', label: 'Owners Estimated Property Value' },
	{ value: 'or_id', label: 'Owner Id (Letter Ref#)' },
    { value: 'p_power', label: 'Power' },
    { value: 'p_address', label: 'Property Address' },
    { value: 'p_city', label: 'Property City' },
    { value: 'p_acres', label: 'Property Size (Acres)' },
    { value: 'p_sqft', label: 'Property Size (Square Feet)' },
    { value: 'p_zip', label: 'Property Zip' },
    { value: 'p_purchase_amount', label: 'Purchase Amount' },
    { value: 'p_closing_company_name_purchase', label: 'Purchase Closing Company' },
    { value: 'p_purchased_on', label: 'Purchase Date' },
    { value: 'p_purchase_closing_costs', label: 'Purchase Total Closing Costs' },
    { value: 'p_agent_name', label: 'Realtor Name' },
    { value: 'p_agent_phone', label: 'Realtor Phone' },
    { value: 'p_restrictions', label: 'Restrictions' },
    { value: 'p_access', label: 'Road Access' },
    { value: 'p_sold_amount', label: 'Sale Amount' },
    { value: 'p_closing_company_name_sale', label: 'Sale Closing Company' },
    { value: 'p_sold_on', label: 'Sale Date' },
    { value: 'p_sale_price', label: 'Sale Price' },
    { value: 'p_sold_closing_costs', label: 'Sale Total Closing Costs' },
    { value: 'p_short_legal', label: 'Short Legal' },
    { value: 'p_status_last_updated', label: 'Status Last Updated' },
    { value: 'p_survey', label: 'Survey' },
    { value: 'tags', label: 'Tag Name' },
    { value: 'tag_ids', label: 'Tag Number' },
    { value: 'p_terrain', label: 'Terrain' },
    { value: 'p_use', label: 'Use' },
    { value: 'p_use_code', label: 'Use Code' },	
    { value: 'p_use_description', label: 'Use Description' },
    { value: 'p_viable', label: 'Viable Seller' },
    { value: 'p_waste_system_requirement', label: 'Waste System Requirement' },
    { value: 'p_water_system_requirement', label: 'Water System Requirement' },
    { value: 'p_owned', label: 'Years Owned' },
    { value: 'p_zoning', label: 'Zoning' }

];

// Initialize advanced search
function initAdvancedSearch() {
    // Load persisted search state
    loadSearchState();
    loadSearchOptions();
	
    // Event listeners
    document.getElementById('add-rule-btn').addEventListener('click', addSearchRule);
    document.getElementById('execute-search-btn').addEventListener('click', executeAdvancedSearch);
    document.getElementById('reset-search-btn').addEventListener('click', resetAdvancedSearch);
	document.getElementById('add-rule-btn').addEventListener('click', addSearchRule);
    
    // Logic buttons
    document.getElementById('logic-and').addEventListener('click', () => setSearchLogic('AND'));
    document.getElementById('logic-or').addEventListener('click', () => setSearchLogic('OR'));
    
    // Check if we have an active search on load
    if (isSearchActive) {
        loadSearchResults();
    }
}

function setSearchLogic(logic) {
    searchLogic = logic;
    document.getElementById('logic-and').classList.toggle('active', logic === 'AND');
    document.getElementById('logic-and').style.background = logic === 'AND' ? '#007bff' : '#6c757d';
    document.getElementById('logic-or').classList.toggle('active', logic === 'OR');
    document.getElementById('logic-or').style.background = logic === 'OR' ? '#007bff' : '#6c757d';
    document.getElementById('logic-description').textContent = logic === 'AND' ? 'All rules must match' : 'Any rule can match';
}

function addSearchRule() {
    const ruleId = Date.now();
    const rule = {
        id: ruleId,
        field: '-1',
        operator: 'equal',
        value: '',
        value2: ''
    };
    searchRules.push(rule);
    renderSearchRule(rule);
}

function renderSearchRule(rule) {
    const container = document.getElementById('search-rules-container');
    const ruleDiv = document.createElement('div');
    ruleDiv.className = 'search-rule';
    ruleDiv.id = `rule-${rule.id}`;
    ruleDiv.style.cssText = 'display: flex; align-items: center; gap: 10px; margin-bottom: 10px; padding: 10px; background: #f9f9f9; border: 1px solid #ddd; border-radius: 4px;';
    
    // Field select
    const fieldSelect = document.createElement('select');
    fieldSelect.className = 'rule-field';
    fieldSelect.style.cssText = 'padding: 5px; min-width: 200px;';
    fieldOptions.forEach(opt => {
        const option = document.createElement('option');
        option.value = opt.value;
        option.textContent = opt.label;
        fieldSelect.appendChild(option);
    });
    fieldSelect.value = rule.field;
    
    // Operator select (will be populated based on field type)
    const operatorSelect = document.createElement('select');
    operatorSelect.className = 'rule-operator';
    operatorSelect.style.cssText = 'padding: 5px; min-width: 150px;';
    
    // Value input container
    const valueContainer = document.createElement('span');
    valueContainer.className = 'rule-value-container';
    valueContainer.style.cssText = 'flex: 1; display: flex; gap: 5px; align-items: center;';
    
    // Delete button (only if more than one rule)
    const deleteBtn = document.createElement('button');
    deleteBtn.innerHTML = '&times;';
    deleteBtn.style.cssText = 'padding: 5px 10px; background: #dc3545; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 18px; line-height: 1;';
    deleteBtn.title = 'Delete Rule';
    deleteBtn.onclick = () => deleteSearchRule(rule.id);
    
    // Event listeners
    fieldSelect.addEventListener('change', () => {
        rule.field = fieldSelect.value;
        updateOperatorOptions(operatorSelect, rule.field);
        updateValueInputs(valueContainer, rule);
    });
    
    operatorSelect.addEventListener('change', () => {
        rule.operator = operatorSelect.value;
        updateValueInputs(valueContainer, rule);
    });
    
    // Initial render
    updateOperatorOptions(operatorSelect, rule.field);
    
    ruleDiv.appendChild(fieldSelect);
    ruleDiv.appendChild(operatorSelect);
    ruleDiv.appendChild(valueContainer);
    if (searchRules.length > 1) {
        ruleDiv.appendChild(deleteBtn);
    }
    
    container.appendChild(ruleDiv);
    updateValueInputs(valueContainer, rule);
}

function updateOperatorOptions(select, field) {
    select.innerHTML = '';
    const type = searchFieldTypes[field] || 'text';
    
    let options = [];
    if (type === 'number') {
        options = [
            { value: 'equal', label: 'equal' },
            { value: 'not_equal', label: 'not equal' },
            { value: 'less', label: 'less' },
            { value: 'less_equal', label: 'less or equal' },
            { value: 'greater', label: 'greater' },
            { value: 'greater_equal', label: 'greater or equal' },
            { value: 'between', label: 'between' },
            { value: 'not_between', label: 'not between' },
            { value: 'is_null', label: 'is null' },
            { value: 'is_not_null', label: 'is not null' }
        ];
    } else if (type === 'date') {
        options = [
            { value: 'is', label: 'is' },
            { value: 'between', label: 'between' },
            { value: 'not_between', label: 'not between' },
            { value: 'is_null', label: 'is null' },
            { value: 'is_not_null', label: 'is not null' }
        ];
    } else if (type === 'boolean') {
        options = [
            { value: 'equal', label: 'equal' },
            { value: 'not_equal', label: 'not equal' }
        ];
    } else {
        // text
        options = [
            { value: 'equal', label: 'equal' },
            { value: 'not_equal', label: 'not equal' },
            { value: 'contains', label: 'contains' },
            { value: 'not_contains', label: 'doesn\'t contain' },
            { value: 'empty', label: 'is empty' },
            { value: 'not_empty', label: 'is not empty' }
        ];
    }
    
    options.forEach(opt => {
        const option = document.createElement('option');
        option.value = opt.value;
        option.textContent = opt.label;
        select.appendChild(option);
    });
}

function updateValueInputs(container, rule) {
    container.innerHTML = '';
    const field = rule.field;  // ADD THIS LINE
    const type = searchFieldTypes[field] || 'text';
    const operator = document.querySelector(`#rule-${rule.id} .rule-operator`)?.value || rule.operator;
    
    // Skip value inputs for null checks
    if (operator === 'is_null' || operator === 'is_not_null' || operator === 'empty' || operator === 'not_empty') {
        rule.value = '';
        rule.value2 = '';
        return;
    }
    
    if (type === 'boolean') {
        // Boolean: Yes/No select
        const select = document.createElement('select');
        select.className = 'rule-value';
        select.style.cssText = 'padding: 5px;';
        const yesOpt = document.createElement('option');
        yesOpt.value = 'Yes';
        yesOpt.textContent = 'Yes';
        const noOpt = document.createElement('option');
        noOpt.value = 'No';
        noOpt.textContent = 'No';
        select.appendChild(yesOpt);
        select.appendChild(noOpt);
        select.value = rule.value || 'Yes';
        select.addEventListener('change', () => rule.value = select.value);
        container.appendChild(select);
        
    } else if (type === 'date') {
        // Date picker(s)
        const input1 = document.createElement('input');
        input1.type = 'text';
        input1.placeholder = 'MM/DD/YYYY';
        input1.className = 'rule-value';
        input1.style.cssText = 'padding: 5px;';
        input1.value = rule.value;
		flatpickr(input1, {dateFormat: 'Y-m-d'});
        input1.addEventListener('change', () => rule.value = input1.value);
        container.appendChild(input1);
        
        if (operator === 'between' || operator === 'not_between') {
            const comma = document.createTextNode(' , ');
            container.appendChild(comma);
            
            const input2 = document.createElement('input');
            input2.type = 'text';
            input2.placeholder = 'MM/DD/YYYY';
            input2.className = 'rule-value2';
            input2.style.cssText = 'padding: 5px;';
            input2.value = rule.value2;
			flatpickr(input2, {dateFormat: 'Y-m-d'});
            input2.addEventListener('change', () => rule.value2 = input2.value);
            container.appendChild(input2);
        }
        
    } else if (type === 'number') {
        // Number input(s)
        const input1 = document.createElement('input');
        input1.type = 'number';
        input1.className = 'rule-value';
        input1.style.cssText = 'padding: 5px; width: 150px;';
        input1.placeholder = 'Value';
        input1.value = rule.value;
        input1.addEventListener('input', () => rule.value = input1.value);
        container.appendChild(input1);
        
        if (operator === 'between' || operator === 'not_between') {
            const comma = document.createTextNode(' , ');
            container.appendChild(comma);
            
            const input2 = document.createElement('input');
            input2.type = 'number';
            input2.className = 'rule-value2';
            input2.style.cssText = 'padding: 5px; width: 150px;';
            input2.placeholder = 'End value';
            input2.value = rule.value2;
            input2.addEventListener('input', () => rule.value2 = input2.value);
            container.appendChild(input2);
        }
        
    } else if (field === 'tag_ids') {  // Changed from field to rule.field (or use const field above)
        // Tag Number dropdown
        const select = document.createElement('select');
        select.className = 'rule-value';
        select.style.cssText = 'padding: 5px; min-width: 200px;';
        
        const defaultOpt = document.createElement('option');
        defaultOpt.value = '';
        defaultOpt.textContent = '-- Select Tag ID --';
        select.appendChild(defaultOpt);
        
        availableTags.forEach(tag => {
            const option = document.createElement('option');
            option.value = tag.tag_id;
            option.textContent = `${tag.tag_id} - ${tag.tag_name}`;
            select.appendChild(option);
        });
        
        select.value = rule.value;
        select.addEventListener('change', () => rule.value = select.value);
        container.appendChild(select);
        
    } else if (field === 'tags') {
        // Tag Name dropdown
        const select = document.createElement('select');
        select.className = 'rule-value';
        select.style.cssText = 'padding: 5px; min-width: 200px;';
        
        const defaultOpt = document.createElement('option');
        defaultOpt.value = '';
        defaultOpt.textContent = '-- Select Tag --';
        select.appendChild(defaultOpt);
        
        availableTags.forEach(tag => {
            const option = document.createElement('option');
            option.value = tag.tag_name;
            option.textContent = tag.tag_name;
            select.appendChild(option);
        });
        
        select.value = rule.value;
        select.addEventListener('change', () => rule.value = select.value);
        container.appendChild(select);
        
    } else if (field === 'p_status') {
        // Status dropdown
        const select = document.createElement('select');
        select.className = 'rule-value';
        select.style.cssText = 'padding: 5px; min-width: 200px;';
        
        const defaultOpt = document.createElement('option');
        defaultOpt.value = '';
        defaultOpt.textContent = '-- Select Status --';
        select.appendChild(defaultOpt);
        
        availableStatuses.forEach(status => {
            const option = document.createElement('option');
            option.value = status.s_status;
            option.textContent = status.s_status;
            select.appendChild(option);
        });
        
        select.value = rule.value;
        select.addEventListener('change', () => rule.value = select.value);
        container.appendChild(select);
        
    } else {
        // Text input
        const input = document.createElement('input');
        input.type = 'text';
        input.className = 'rule-value';
        input.style.cssText = 'padding: 5px; flex: 1;';
        input.placeholder = 'Enter value...';
        input.value = rule.value;
        input.addEventListener('input', () => rule.value = input.value);
        container.appendChild(input);
    }
}

function deleteSearchRule(ruleId) {
    searchRules = searchRules.filter(r => r.id !== ruleId);
    const element = document.getElementById(`rule-${ruleId}`);
    if (element) element.remove();
    
    // Update delete buttons visibility
    const deleteButtons = document.querySelectorAll('#search-rules-container .search-rule button');
    deleteButtons.forEach((btn, index) => {
        btn.style.display = searchRules.length > 1 ? 'inline-block' : 'none';
    });
}

function resetAdvancedSearch() {
    searchRules = [];
    searchLogic = 'AND';
    document.getElementById('search-rules-container').innerHTML = '';
    document.getElementById('logic-and').click();
    addSearchRule(); // Add one empty rule
}

function openAdvancedSearch() {
    // If first time opening, add one empty rule
    if (searchRules.length === 0) {
        addSearchRule();
    }
    openPopup('advanced-search-popup');
}

function executeAdvancedSearch() {
    // Collect current values from DOM
    const ruleElements = document.querySelectorAll('.search-rule');
    const rules = [];
    
    ruleElements.forEach(el => {
        const field = el.querySelector('.rule-field').value;
        if (field === '-1') return; // Skip empty selections
        
        const operator = el.querySelector('.rule-operator').value;
        const valueInput = el.querySelector('.rule-value');
        const value2Input = el.querySelector('.rule-value2');
        
        let value = valueInput ? valueInput.value : '';
        let value2 = value2Input ? value2Input.value : '';
        
        // Skip incomplete rules
        if ((operator !== 'is_null' && operator !== 'is_not_null' && operator !== 'empty' && operator !== 'not_empty') && !value) {
            return;
        }
        
        rules.push({ field, operator, value, value2 });
    });
    
    if (rules.length === 0) {
        document.getElementById('search-error').textContent = 'Please add at least one valid search rule';
        document.getElementById('search-error').style.display = 'block';
        return;
    }
    
    document.getElementById('search-error').style.display = 'none';
    
    // Save search state
    isSearchActive = true;
    saveSearchState(rules, searchLogic);
    
    // Close popup and load results
    closePopup();
    currentPage = 1;
    loadSearchResults();
}

async function loadSearchResults() {
    try {
        const searchState = JSON.parse(localStorage.getItem('advancedSearchState'));
        if (!searchState) return;
        
        const columnsParam = visibleColumns.join(',');
        const requestData = {
            logic: searchState.logic,
            rules: searchState.rules,
            page: currentPage,
            per_page: perPage,
            columns: columnsParam,
            sort_by: sortBy,
            sort_dir: sortDir
        };
        
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestData)
        });
        
        const data = await response.json();
        
        if (!data.success) {
            alert('Search error: ' + (data.error || 'Unknown error'));
            return;
        }
        
        // Update UI to show search is active
        // Simple search active - no indicator shown per spec
        
        // Render results using existing table logic
        renderTable(data.properties);
		setClearSearchButtonActive(true);
        updatePagination(data.pagination);
        totalPages = data.pagination.total_pages;
        
        // Update header checkbox
        const allVisibleSelected = data.properties.every(row => selectedRecords.has(row.p_id));
        const headerCheckbox = document.getElementById('header-checkbox');
        if (headerCheckbox) headerCheckbox.checked = allVisibleSelected;
        
    } catch (error) {
        console.error('Search error:', error);
        alert('Error executing search');
    }
}

async function performSync() {
    openPopup('sync-modal');
    const progressBar = document.getElementById('sync-progress-bar');
    const statusText = document.getElementById('sync-status-text');
    const results = document.getElementById('sync-results');
    
    // Start timer
    const startTime = Date.now();
    
    // Helper function to format elapsed time
    const getElapsedTime = () => {
        const elapsed = Date.now() - startTime;
        const seconds = Math.floor(elapsed / 1000);
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = seconds % 60;
        
        if (minutes > 0) {
            return `${minutes}m ${remainingSeconds}s`;
        } else {
            return `${seconds}s`;
        }
    };
    
    // Reset display
    results.style.display = 'none';
    results.innerHTML = '';
    
    try {
        statusText.textContent = 'Syncing data to cloud...';
        progressBar.style.width = '50%';
        
        const response = await fetch('/api/sync', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({direction: 'bidirectional'})
        });
        
        progressBar.style.width = '100%';
        
        if (response.ok) {
            const data = await response.json();
            
            let html = '<h4 style="margin-top: 0; color: #28a745;">Sync Complete</h4>';
            html += `<p>✓ Properties pushed: ${data.properties_pushed || 0}</p>`;
            html += `<p>✓ Properties pulled: ${data.properties_pulled || 0}</p>`;
            html += `<p>✓ Owners pushed: ${data.owners_pushed || 0}</p>`;
            html += `<p>✓ Owners pulled: ${data.owners_pulled || 0}</p>`;
			html += `<p>✓ Links pushed: ${data.links_pushed || 0}</p>`;
            html += `<p>✓ Links pulled: ${data.links_pulled || 0}</p>`;
            html += `<p>✓ Files uploaded: ${data.uploaded || 0}</p>`;
            html += `<p>✓ Files downloaded: ${data.downloaded || 0}</p>`;
            html += `<hr style="margin: 10px 0; border: none; border-top: 1px solid #dee2e6;">`;
            html += `<p style="font-size: 0.9em; color: #6c757d; margin-bottom: 5px;"><strong>Reference Tables:</strong></p>`;
            html += `<p style="font-size: 0.9em; margin-left: 10px;">• Statuses synced: ${data.statuses_synced || 0}</p>`;
            html += `<p style="font-size: 0.9em; margin-left: 10px;">• Tags synced: ${data.tags_synced || 0}</p>`;
            html += `<p style="font-size: 0.9em; margin-left: 10px;">• Companies synced: ${data.companies_synced || 0}</p>`;
            html += `<p style="font-size: 0.9em; margin-left: 10px;">• Templates synced: ${data.templates_synced || 0}</p>`;
            
            if (data.conflicts && data.conflicts.length > 0) {
                html += `<p style="color: #ffc107;">⚠ Conflicts: ${data.conflicts.length}</p>`;
            }
            
            if (data.errors && data.errors.length > 0) {
                html += `<p style="color: #dc3545;">✗ Errors: ${data.errors.length}</p>`;
                // Show first 3 errors
                html += '<ul style="color: #dc3545; font-size: 12px;">';
                data.errors.slice(0, 3).forEach(err => {
                    html += `<li>${err}</li>`;
                });
                if (data.errors.length > 3) {
                    html += `<li>...and ${data.errors.length - 3} more</li>`;
                }
                html += '</ul>';
            }
            
            // Add elapsed time
            html += `<p style="margin-top: 15px; padding-top: 10px; border-top: 1px solid #ddd; color: #666; font-size: 13px;"><strong>Time elapsed:</strong> ${getElapsedTime()}</p>`;
            
            results.innerHTML = html;
            results.style.display = 'block';
            statusText.textContent = 'Sync completed successfully';
            
            // Update status indicator
            updateSyncIndicator(true);
        } else {
            const errorData = await response.json();
            statusText.textContent = 'Sync failed';
            statusText.style.color = '#dc3545';
            
            // Add elapsed time even on failure
            const elapsed = getElapsedTime();
            results.innerHTML = `<p style="color: #dc3545;">Error: ${errorData.error || 'Unknown error'}</p><p style="color: #666; font-size: 13px; margin-top: 10px;"><strong>Time elapsed:</strong> ${elapsed}</p>`;
            results.style.display = 'block';
        }
    } catch (error) {
        progressBar.style.width = '100%';
        statusText.textContent = 'Offline - Will retry when connected';
        statusText.style.color = '#ffc107';
        
        // Add elapsed time even on connection error
        const elapsed = getElapsedTime();
        results.innerHTML = `<p style="color: #ffc107;">Could not connect to server. Please check your connection.</p><p style="color: #666; font-size: 13px; margin-top: 10px;"><strong>Time elapsed:</strong> ${elapsed}</p>`;
        results.style.display = 'block';
        updateSyncIndicator(false);
    }
}

function closeSyncModal() {
    closePopup();
}

function updateSyncIndicator(isOnline) {
    const indicator = document.querySelector('.sync-dot');
    const text = document.querySelector('.sync-text');
    
    if (isOnline) {
        indicator.style.background = '#28a745';
        text.textContent = 'Synced';
    } else {
        indicator.style.background = '#ffc107';
        text.textContent = 'Offline';
    }
}

// Check sync status periodically
setInterval(async () => {
    try {
        const response = await fetch('/api/sync/status');
        if (response.ok) {
            const data = await response.json();
            const countSpan = document.getElementById('sync-pending-count');
            
            if (data.properties_pending > 0 || data.files_pending > 0) {
                countSpan.textContent = `${data.properties_pending} pending`;
                countSpan.style.display = 'inline';
                updateSyncIndicator(false);
            } else {
                countSpan.style.display = 'none';
                updateSyncIndicator(true);
            }
        }
    } catch (e) {
        updateSyncIndicator(false);
    }
}, 30000); // Check every 30 seconds

function showSearchActiveIndicator() {
    // Add or update search indicator in the UI
    let indicator = document.getElementById('search-active-indicator');
    if (!indicator) {
        indicator = document.createElement('div');
        indicator.id = 'search-active-indicator';
        indicator.style.cssText = 'background: #e3f2fd; border: 1px solid #2196f3; color: #1976d2; padding: 10px; margin: 10px 0; border-radius: 4px; display: flex; justify-content: space-between; align-items: center;';
        
        const filterMenu = document.querySelector('.filter-menu');
        filterMenu.parentNode.insertBefore(indicator, filterMenu);
    }
    
    indicator.innerHTML = `
        <span><strong>Advanced Search Active</strong> - Showing filtered results</span>
        <button onclick="clearAdvancedSearch()" style="background: #dc3545; color: white; border: none; padding: 5px 15px; border-radius: 4px; cursor: pointer;">Clear Search</button>
    `;
}

function clearAdvancedSearch() {
    isSearchActive = false;
    localStorage.removeItem('advancedSearchState');
    
    const indicator = document.getElementById('search-active-indicator');
    if (indicator) indicator.remove();
    
    // Reset to default filter view
    currentFilter = 'all_in_process';
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.dataset.filter === 'all_in_process') btn.classList.add('active');
    });
    
    currentPage = 1;
    loadProperties();
}

function saveSearchState(rules, logic) {
    localStorage.setItem('advancedSearchState', JSON.stringify({
        rules: rules,
        logic: logic,
        timestamp: new Date().toISOString()
    }));
}

function loadSearchState() {
    const saved = localStorage.getItem('advancedSearchState');
    if (saved) {
        const state = JSON.parse(saved);
        searchRules = state.rules.map(r => ({ ...r, id: Date.now() + Math.random() })); // Restore with new IDs
        searchLogic = state.logic;
        isSearchActive = true;
    }
}

// Persistence for filter and sort
function saveDashboardState() {
    localStorage.setItem('dashboardState', JSON.stringify({
        currentFilter: currentFilter,
        sortBy: sortBy,
        sortDir: sortDir,
        currentPage: currentPage,
        perPage: perPage
    }));
}

function loadDashboardState() {
    const saved = localStorage.getItem('dashboardState');
    if (saved) {
        const state = JSON.parse(saved);
        currentFilter = state.currentFilter || 'all_in_process';
        sortBy = state.sortBy || 'p_id';
        sortDir = state.sortDir || 'asc';
        currentPage = state.currentPage || 1;
        perPage = state.perPage || 10;
        
        // Update UI
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.classList.remove('active');
            if (btn.dataset.filter === currentFilter) btn.classList.add('active');
        });
        
        document.getElementById('per-page').value = perPage;
    }
}

// Modify existing functions to save state
const originalLoadProperties = loadProperties;
loadProperties = async function() {
    saveDashboardState();
    if (isSearchActive) {
        await loadSearchResults();
    } else {
        await originalLoadProperties();
    }
};

// Update handleMenuAction for Advanced Search
function handleMenuAction(event) {
    const action = event.target.dataset.action;
    
    switch(action) {
        case 'new-offer-request':
            openNewOfferPopup();
            break;
        case 'add-record':
            window.location.href = '/property/new';
            break;
        case 'import':
            window.location.href = '/import';
            break;
        case 'export-all':
            exportAllData();
            break;
        case 'mailing':
            openMailingPopup();
            break;
        case 'generate-docs':
            if (selectedRecords.size === 0) {
                alert('No records selected for document generation');
                return;
            }
            const propertyIds = Array.from(selectedRecords).join(',');
            window.location.href = `/documents?properties=${propertyIds}`;
            break;
        case 'status':
            window.location.href = '/status-management';
            break;
        case 'tags':
            window.location.href = '/tags';
            break;
        case 'company-info':
            window.location.href = '/company';
            break;
        case 'advanced-search':
            openAdvancedSearch();
            break;
    }
}

// ============================================
// SIMPLE SEARCH FUNCTIONS
// ============================================

// Field type mapping for simple search
const simpleSearchFieldTypes = {
    'keyword': 'keyword',
    'p_apn': 'text',
    'p_status': 'dropdown',
    'p_id': 'equals',
    'or_fname': 'text',
    'or_lname': 'text',
    'o_company': 'text',
    'or_phone': 'text',
    'or_m_zip': 'text',
    'p_county': 'text',
    'p_state': 'dropdown',
    'p_zip': 'text',
    'p_acres': 'equals',
    'p_create_time': 'date_range',
    'p_last_updated': 'date_range',
    'p_offer_accept_date': 'special',
    'p_aquired': 'dropdown',
    'tags': 'dropdown'
};

// Options for dropdown fields
const simpleSearchOptions = {
    'p_aquired': ['Purchased', 'Inherited', 'Gifted', 'Probate Needed'],
    'p_state': ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC', 'PR', 'VI', 'GU', 'AS', 'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'ON', 'PE', 'QC', 'SK', 'NT', 'YT', 'NU']
};

// Helper to convert YYYY-MM-DD (from HTML date input) to MM/DD/YYYY (for backend comparison)
function convertDateFormat(dateString) {
    if (!dateString || !dateString.includes('-')) return dateString;
    const parts = dateString.split('-');
    if (parts.length === 3) {
        return `${parts[1]}/${parts[2]}/${parts[0]}`;
    }
    return dateString;
}

// Helper to set Clear Search button visual state
function setClearSearchButtonActive(active) {
    const btn = document.querySelector('.clear-search-btn');
    if (btn) {
        if (active) {
            btn.classList.add('search-active');
        } else {
            btn.classList.remove('search-active');
        }
    }
}

// Update the input field based on selected search field
function updateSimpleSearchInput() {
    const field = document.getElementById('simpleSearchField').value;
    const container = document.getElementById('simpleSearchInputContainer');
    const equalsSign = document.getElementById('simpleSearchEquals');

    container.innerHTML = '';

    if (!field) {
        const input = document.createElement('input');
        input.type = 'text';
        input.id = 'simpleSearchValue';
        input.placeholder = 'Enter search term...';
        input.className = 'search-text-input';
        container.appendChild(input);
        equalsSign.style.display = 'inline';
        return;
    }

    const fieldType = simpleSearchFieldTypes[field] || 'text';

    if (fieldType === 'keyword') {
        equalsSign.style.display = 'inline';
        const input = document.createElement('input');
        input.type = 'text';
        input.id = 'simpleSearchValue';
        input.placeholder = 'Search across multiple fields...';
        input.className = 'search-text-input';
        container.appendChild(input);

    } else if (fieldType === 'special') {
        // Expired Offer - no input needed
        equalsSign.style.display = 'none';
        const msg = document.createElement('span');
        msg.textContent = '(Click Search to find expired offers)';
        msg.style.color = '#718096';
        msg.style.fontStyle = 'italic';
        container.appendChild(msg);
        
        // Hidden field so logic doesn't break
        const hidden = document.createElement('input');
        hidden.type = 'hidden';
        hidden.id = 'simpleSearchValue';
        hidden.value = 'expired';
        container.appendChild(hidden);

    } else if (fieldType === 'date_range') {
        // Date Created, Last Updated, etc.
        equalsSign.style.display = 'none';

        const rangeContainer = document.createElement('div');
        rangeContainer.style.display = 'flex';
        rangeContainer.style.alignItems = 'center';
        rangeContainer.style.gap = '8px';

        const fromInput = document.createElement('input');
        fromInput.type = 'date';
        fromInput.id = 'simpleSearchValueFrom';
        fromInput.className = 'search-date-input';
        fromInput.placeholder = 'From';

        const toLabel = document.createElement('span');
        toLabel.textContent = 'to';
        toLabel.style.color = '#4a5568';

        const toInput = document.createElement('input');
        toInput.type = 'date';
        toInput.id = 'simpleSearchValueTo';
        toInput.className = 'search-date-input';
        toInput.placeholder = 'To';

        // Initialize flatpickr if available
        if (typeof flatpickr !== 'undefined') {
            flatpickr(fromInput, {dateFormat: 'Y-m-d'});
            flatpickr(toInput, {dateFormat: 'Y-m-d'});
        }

        rangeContainer.appendChild(fromInput);
        rangeContainer.appendChild(toLabel);
        rangeContainer.appendChild(toInput);
        container.appendChild(rangeContainer);

    } else if (fieldType === 'dropdown') {
        equalsSign.style.display = 'inline';
        const select = document.createElement('select');
        select.id = 'simpleSearchValue';
        select.className = 'search-select-input';

        const defaultOpt = document.createElement('option');
        defaultOpt.value = '';
        defaultOpt.textContent = '-- Select --';
        select.appendChild(defaultOpt);

        if (field === 'tags') {
            availableTags.forEach(tag => {
                const opt = document.createElement('option');
                opt.value = tag.tag_name;
                opt.textContent = tag.tag_name;
                select.appendChild(opt);
            });
        } else if (field === 'p_status') {
            availableStatuses.forEach(status => {
                const opt = document.createElement('option');
                opt.value = status.s_status;
                opt.textContent = status.s_status;
                select.appendChild(opt);
            });
        } else if (simpleSearchOptions[field]) {
            simpleSearchOptions[field].forEach(optValue => {
                const opt = document.createElement('option');
                opt.value = optValue;
                opt.textContent = optValue;
                select.appendChild(opt);
            });
        }

        container.appendChild(select);

    } else if (fieldType === 'equals') {
        equalsSign.style.display = 'inline';
        const input = document.createElement('input');
        input.type = field === 'p_id' ? 'number' : 'text';
        input.id = 'simpleSearchValue';
        input.placeholder = 'Enter exact value...';
        input.className = 'search-text-input';
        container.appendChild(input);

    } else {
        // Default text
        equalsSign.style.display = 'inline';
        const input = document.createElement('input');
        input.type = 'text';
        input.id = 'simpleSearchValue';
        input.placeholder = 'Enter search term...';
        input.className = 'search-text-input';
        container.appendChild(input);
    }
}

// Execute simple search
function executeSimpleSearch() {
    const field = document.getElementById('simpleSearchField').value;
    
    if (!field) {
        alert('Please select a search field');
        return;
    }

    // Build search request data
    let requestData = {
        columns: visibleColumns.join(','),
        page: 1,
        per_page: perPage,
        sort_by: sortBy,
        sort_dir: sortDir,
        rules: []
    };

    const fieldType = simpleSearchFieldTypes[field] || 'text';
    
    // Helper to convert YYYY-MM-DD to MM/DD/YYYY for backend comparison
    const formatDateForBackend = (dateStr) => {
        return dateStr; // Pass through as-is
    };

    // Handle different field types
    if (fieldType === 'keyword') {
        const value = document.getElementById('simpleSearchValue')?.value || '';
        if (!value.trim()) {
            alert('Please enter a search term');
            return;
        }
        requestData.rules.push({
            field: 'all_fields',
            operator: 'contains',
            value: value.trim()
        });

    } else if (fieldType === 'special') {
    // Expired Offer: p_offer_accept_date < today, excluding nulls
        const today = new Date();
        const formattedToday = `${today.getFullYear()}-${(today.getMonth()+1).toString().padStart(2,'0')}-${today.getDate().toString().padStart(2,'0')}`;    

    // 1. Exclude NULL dates (this is all you need!)
		requestData.rules.push({
			field: 'p_offer_accept_date',
			operator: 'is_not_null',
			value: '1'
		});
    
    // 2. Find expired dates
		requestData.rules.push({
			field: 'p_offer_accept_date',
			operator: 'less',
			value: formattedToday
		});

    } else if (fieldType === 'date_range') {
        const fromInput = document.getElementById('simpleSearchValueFrom');
        const toInput = document.getElementById('simpleSearchValueTo');
        const fromVal = fromInput ? fromInput.value : '';
        const toVal = toInput ? toInput.value : '';

        if (!fromVal && !toVal) {
            alert('Please enter at least one date');
            return;
        }

        // Date Created (p_create_time) and Last Updated (p_last_updated) both use this
        if (fromVal) {
            requestData.rules.push({
                field: field,
                operator: 'greater_equal',
                value: formatDateForBackend(fromVal)
            });
        }
        if (toVal) {
            requestData.rules.push({
                field: field,
                operator: 'less_equal',
                value: formatDateForBackend(toVal)
            });
        }

    } else if (fieldType === 'equals') {
        const value = document.getElementById('simpleSearchValue')?.value || '';
        if (!value.trim()) {
            alert('Please enter a value');
            return;
        }
        requestData.rules.push({
            field: field,
            operator: 'equal',
            value: value.trim()
        });

    } else if (fieldType === 'dropdown') {
        const value = document.getElementById('simpleSearchValue')?.value || '';
        if (!value) {
            alert('Please select a value');
            return;
        }
        requestData.rules.push({
            field: field,
            operator: 'equal',
            value: value
        });

    } else {
        // Default text search
        const value = document.getElementById('simpleSearchValue')?.value || '';
        if (!value.trim()) {
            alert('Please enter a search term');
            return;
        }
        requestData.rules.push({
            field: field,
            operator: 'contains',
            value: value.trim()
        });
    }

    // Validate rules exist
    if (requestData.rules.length === 0) {
        alert('No valid search criteria provided');
        return;
    }

    // Save state for persistence
    localStorage.setItem('advancedSearchState', JSON.stringify({
        rules: requestData.rules,
        logic: 'AND',
        timestamp: new Date().toISOString(),
        isSimpleSearch: true,
        simpleField: field
    }));
    
    // Save UI state
    localStorage.setItem('simpleSearchUI', JSON.stringify({
        field: field,
        fieldType: fieldType,
        timestamp: new Date().toISOString()
    }));
    
    isSearchActive = true;
    setClearSearchButtonActive(true);

    // Execute search
    fetch('/api/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestData)
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            renderTable(data.properties);
            updatePagination(data.pagination);
            totalPages = data.pagination.total_pages;

            const filterTitle = document.getElementById('filterTitle');
            if (filterTitle) filterTitle.textContent = 'Search Results';

            const allVisibleSelected = data.properties.every(row => selectedRecords.has(row.p_id));
            const headerCheckbox = document.getElementById('header-checkbox');
            if (headerCheckbox) headerCheckbox.checked = allVisibleSelected;
        } else {
            alert('Search error: ' + (data.error || 'Unknown error'));
        }
    })
    .catch(error => {
        console.error('Search error:', error);
        alert('Search error: ' + error.message);
    });
}

// Clear simple search and return to default view
function clearSimpleSearch() {
    // Reset simple search fields
    document.getElementById('simpleSearchField').value = '';
    updateSimpleSearchInput();

    // Remove search active indicator
    const indicator = document.getElementById('search-active-indicator');
    if (indicator) indicator.remove();

    // CRITICAL FIX: Reset search active state
    isSearchActive = false;
    localStorage.removeItem('advancedSearchState');
    localStorage.removeItem('simpleSearchUI'); // Clear simple search UI state too

    // Reset button color
    setClearSearchButtonActive(false);

    // Reset to default view (All In Process)
    currentFilter = 'all_in_process';
    currentPage = 1;

    // Update filter buttons UI
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.dataset.filter === 'all_in_process') {
            btn.classList.add('active');
        }
    });

    // Update title
    const filterTitle = document.getElementById('filterTitle');
    if (filterTitle) filterTitle.textContent = 'Properties';

    // Load properties
    loadProperties();
}
