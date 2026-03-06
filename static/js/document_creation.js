// document_creation.js - Round 10 Fixes

let selectedProperties = [];
let templates = [];
let selectedTemplate = null;
let selectedMailingType = null;

document.addEventListener('DOMContentLoaded', function() {
    // Get selected property IDs from URL params
    const urlParams = new URLSearchParams(window.location.search);
    const propertyIds = urlParams.get('properties') ? urlParams.get('properties').split(',') : [];
    
    if (propertyIds.length === 0) {
        alert('No properties selected. Please select properties from the dashboard.');
        window.location.href = '/';
        return;
    }
    
    selectedProperties = propertyIds.map(id => parseInt(id));
    
    loadTemplates();
    loadPropertySummary();
    setupEventListeners();
});

// Helper to handle downloads in desktop app vs browser
async function downloadFile(filename) {
    // Check if running in PyWebView desktop app
    if (window.pywebview && window.pywebview.api) {
        try {
            const result = await window.pywebview.api.download_file(filename);
            if (result.success) {
                console.log('File saved to:', result.path);
                // Optional: Show success message
                if (typeof showPopup === 'function') {
                    showPopup('File saved successfully to: ' + result.path);
                } else {
                    alert('File saved to: ' + result.path);
                }
            } else if (result.cancelled) {
                console.log('User cancelled download');
            } else {
                console.error('Download error:', result.error);
                alert('Download error: ' + result.error);
            }
        } catch (err) {
            console.error('API error:', err);
            // Fallback to browser behavior
            window.open('/static/exports/' + filename, '_blank');
        }
    } else {
        // Normal browser mode - use standard download
        window.open('/static/exports/' + filename, '_blank');
    }
}

function setupEventListeners() {
    document.getElementById('back-btn').addEventListener('click', () => {
        window.location.href = '/';
    });
    
    document.getElementById('cancel-btn').addEventListener('click', () => {
        window.location.href = '/';
    });
    
    document.getElementById('generate-btn').addEventListener('click', generateDocuments);
	
	// Update Status button
    document.getElementById('update-status-btn').addEventListener('click', openChangeStatusPopup);
    
    // Popup close handlers
    document.querySelector('.close-popup').addEventListener('click', closePopup);
    document.getElementById('popup-overlay').addEventListener('click', closePopup);
    document.querySelector('#change-status-popup .btn-cancel').addEventListener('click', closePopup);
    
    // Mark as Mailed button
    document.getElementById('mark-as-mailed-btn').addEventListener('click', markAsMailed);
    
    // Template selection (using event delegation)
    document.getElementById('template-list').addEventListener('click', function(e) {
        if (e.target.classList.contains('btn-select')) {
            const item = e.target.closest('.template-item');
            if (item && item.dataset.templateId) {
                selectTemplate(parseInt(item.dataset.templateId));
            }
        }
    });
    
    // Mailing list selection (using event delegation)
    document.getElementById('mailing-list').addEventListener('click', function(e) {
        if (e.target.classList.contains('btn-mailing') || e.target.closest('.template-item')) {
            const item = e.target.closest('.template-item');
            if (item && item.dataset.mailingType) {
                selectMailingType(item.dataset.mailingType);
            }
        }
    });
}

function selectMailingType(type) {
    // Clear template selection
    selectedTemplate = null;
    selectedMailingType = type;
    
    // Update UI - clear template selections
    document.querySelectorAll('#template-list .template-item').forEach(item => {
        item.classList.remove('selected');
    });
    
    // Highlight selected mailing type
    document.querySelectorAll('#mailing-list .template-item').forEach(item => {
        item.classList.remove('selected');
    });
    
    const selectedItem = document.querySelector(`[data-mailing-type="${type}"]`);
    if (selectedItem) {
        selectedItem.classList.add('selected');
    }
    
    // Enable generate button
    document.getElementById('generate-btn').disabled = false;
    document.getElementById('generate-btn').textContent = 'Generate List';
}

async function markAsMailed() {
    if (selectedProperties.length === 0) {
        alert('No properties selected');
        return;
    }
    
    try {
        const response = await fetch('/api/properties/mark-as-mailed', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                property_ids: selectedProperties
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            alert(`Success! Updated ${data.updated} properties with dates:\n` +
                  `Mail Date: ${data.dates.p_m_date}\n` +
                  `Offer Accept By: ${data.dates.p_offer_accept_date}\n` +
                  `Contract Expires: ${data.dates.p_contract_expires_date}`);
        } else {
            alert('Error: ' + data.error);
        }
    } catch (error) {
        console.error('Mark as mailed error:', error);
        alert('Error marking properties as mailed');
    }
}

async function generateDocuments() {
    if (selectedMailingType) {
        // Generate mailing list CSV
        await generateMailingList(selectedMailingType);
    } else if (selectedTemplate) {
        // Generate Word document
        await generateWordDocument();
    } else {
        alert('Please select a template or mailing list type');
    }
}

async function generateMailingList(type) {
    const modal = document.getElementById('progress-modal');
    modal.style.display = 'flex';
    document.getElementById('progress-text').textContent = 'Generating mailing list...';
    
    try {
        // Fetch full property data for selected IDs
        const properties = await Promise.all(
            selectedProperties.map(async (p_id) => {
                const response = await fetch(`/api/properties/${p_id}`);
                if (!response.ok) return null;
                return response.json();
            })
        );
        
        const validProperties = properties.filter(p => p !== null);
        
        if (validProperties.length === 0) {
            throw new Error('No valid properties found');
        }
        
        // Prepare export data
        const exportData = validProperties.map(prop => {
            const o_type = prop.o_type || '';
            const or_fname = prop.or_fname || '';
            const or_lname = prop.or_lname || '';
            const o_company = prop.o_company || '';
            
            let or_name, or_greeting;
            if (o_type === 'Company') {
                or_name = o_company;
                or_greeting = "To whom it may concern,";
            } else {
                or_name = `${or_fname} ${or_lname}`.trim();
                or_greeting = `Dear ${or_fname},`;
            }
            
            // Use generation timestamp for m_date
            const today = new Date();
            const m_date = today.toLocaleDateString('en-US', {
                month: 'long',
                day: 'numeric',
                year: 'numeric'
            }).replace(/(\d+),/, (match, p1) => ` ${p1},`);
            
            const baseRecord = {
                or_id: prop.or_id,
                p_state: prop.p_state,
                p_county: prop.p_county,
                p_apn: prop.p_apn,
                or_greeting: or_greeting,
                or_name: or_name,
                or_m_address: prop.or_m_address,
                or_m_city: prop.or_m_city,
                or_m_state: prop.or_m_state,
                or_m_zip: prop.or_m_zip,
                p_longstate: prop.p_longstate || getLongState(prop.p_state),
                m_date: '="' + m_date + '"'
            };
            
            if (type === 'email') {
                baseRecord.or_email = prop.or_email;
            }
            
            return baseRecord;
        });
        
        // Determine fields
        const fields = type === 'usmail' 
            ? ['or_id', 'p_state', 'p_county', 'p_apn', 'or_greeting', 'or_name', 
               'or_m_address', 'or_m_city', 'or_m_state', 'or_m_zip', 'p_longstate', 'm_date']
            : ['or_id', 'p_state', 'p_county', 'p_apn', 'or_greeting', 'or_name', 'or_email',
               'or_m_address', 'or_m_city', 'or_m_state', 'or_m_zip', 'p_longstate', 'm_date'];
        
        // Create CSV content
        let csv = fields.join(',') + '\n';
        
        exportData.forEach(row => {
            const values = fields.map(field => {
                const val = String(row[field] || '');
                // Escape values containing commas or quotes
                if (val.includes(',') || val.includes('"') || val.includes('\n')) {
                    return '"' + val.replace(/"/g, '""') + '"';
                }
                return val;
            });
            csv += values.join(',') + '\n';
        });
        
        // Generate filename
        const timestamp = new Date().toISOString().slice(0,16).replace(/[-:]/g, '').replace('T', '_');
        const filename = type === 'usmail' ? `Mailing_${timestamp}.csv` : `Emailing_${timestamp}.csv`;
        
        // Create blob
        const blob = new Blob([csv], { type: 'text/csv' });
        
        // FIXED: Use readAsDataURL pattern matching other functions
        if (window.pywebview && window.pywebview.api) {
            const reader = new FileReader();
            reader.onload = async function() {
                try {
                    const dataUrl = reader.result; // data:text/csv;base64,...
                    const result = await window.pywebview.api.save_download_file(filename, dataUrl);
                    
                    if (result.success) {
                        console.log('Mailing list saved to:', result.path);
						alert('Mailing list saved to: ' + result.path);
                    } else if (result.cancelled) {
                        console.log('User cancelled save');
                    } else {
                        console.error('Save error:', result.error);
                        alert('Error saving mailing list: ' + (result.error || 'Unknown error'));
                    }
                } catch (err) {
                    console.error('Error in save_download_file:', err);
                    alert('Error saving file: ' + err.message);
                }
            };
            reader.onerror = function(err) {
                console.error('FileReader error:', err);
                alert('Error reading file data');
            };
            reader.readAsDataURL(blob);
        } else {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
        }
        
        modal.style.display = 'none';
        
    } catch (error) {
        console.error('Export error:', error);
        modal.style.display = 'none';
        alert('Error generating mailing list: ' + error.message);
    }
}

async function generateWordDocument() {
    const modal = document.getElementById('progress-modal');
    modal.style.display = 'flex';
    document.getElementById('progress-text').textContent = 'Generating documents...';
    
    try {
        console.log('[DEBUG] Starting document generation');
        const response = await fetch('/api/documents/generate', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                template_id: selectedTemplate,
                property_ids: selectedProperties
            })
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Generation failed');
        }
        
        // Get filename from header or generate default
        const disposition = response.headers.get('content-disposition');
        let filename = 'document.docx';
        if (disposition && disposition.includes('filename=')) {
            filename = disposition.split('filename=')[1].replace(/["']/g, '');
        }
        console.log('[DEBUG] Generated filename:', filename);
        
        // Get blob
        const blob = await response.blob();
        console.log('[DEBUG] Received blob:', blob.size, 'bytes, type:', blob.type);
        
        // Desktop app vs browser handling - FIXED
        if (window.pywebview && window.pywebview.api) {
            console.log('[DEBUG] Desktop mode detected');
            const reader = new FileReader();
            reader.onload = async function() {
                try {
                    const dataUrl = reader.result;
                    console.log('[DEBUG] Calling save_download_file with data URL');
                    const result = await window.pywebview.api.save_download_file(filename, dataUrl);
                    console.log('[DEBUG] save_download_file result:', result);
                    
                    if (result.success) {
                        console.log('Document saved to:', result.path);
						alert('Document saved to: ' + result.path); 
                    } else if (result.cancelled) {
                        console.log('User cancelled save');
                    } else {
                        console.error('Save error:', result.error);
                        alert('Error saving document: ' + (result.error || 'Unknown error'));
                    }
                } catch (err) {
                    console.error('Error calling save_download_file:', err);
                    alert('Error saving file: ' + err.message);
                }
            };
            reader.onerror = function(err) {
                console.error('FileReader error:', err);
                alert('Error reading generated document');
            };
            reader.readAsDataURL(blob);
        } else {
            // Browser mode
            console.log('[DEBUG] Browser mode');
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);
        }
        
        modal.style.display = 'none';
        
    } catch (error) {
        console.error('Generation error:', error);
        modal.style.display = 'none';
        alert('Error generating documents: ' + error.message);
    }
}

// Helper function for state conversion
function getLongState(abbr) {
    const states = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
        'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
        'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
        'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
        'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire',
        'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina',
        'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
        'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
        'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington',
        'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'
    };
    return states[abbr] || abbr;
}

async function loadTemplates() {
    try {
        const response = await fetch('/api/documents/templates');
        const data = await response.json();
        
        // Filter out "Multi" templates from UI
        let filtered = data.templates.filter(template => !template.name.includes('Multi'));
        
        // Explicit ordering: Neutral, Blind Offer, 2nd Offer, Contract, Offer
        const orderMap = {
            'Neutral Letter': 1,
            'Blind Offer Letter': 2,
            '2nd Offer Letter': 3,
            'Contract Template': 4,
            'Offer Letter': 5,
            'Postcard Template': 6
        };
        
        templates = filtered.sort((a, b) => {
            return (orderMap[a.name] || 999) - (orderMap[b.name] || 999);
        });
        
        const templateList = document.getElementById('template-list');
        templateList.innerHTML = '';
        
        templates.forEach(template => {
            const item = document.createElement('div');
            item.className = 'template-item';
            item.dataset.templateId = template.template_id; // FIX: Added data attribute
            item.innerHTML = `
                <span class="template-name">${template.name}</span>
                <button class="btn-select">Select</button>
            `;
            
            templateList.appendChild(item);
        });
    } catch (error) {
        console.error('Error loading templates:', error);
        alert('Error loading document templates');
    }
}

function selectTemplate(templateId) {
    // Clear mailing selection
    selectedMailingType = null;
    selectedTemplate = templateId;
    
    // Update UI - clear mailing selections
    document.querySelectorAll('#mailing-list .template-item').forEach(item => {
        item.classList.remove('selected');
    });
    
    // Update UI - clear other template selections
    document.querySelectorAll('#template-list .template-item').forEach(item => {
        item.classList.remove('selected');
    });
    
    // Highlight selected template
    const selectedItem = document.querySelector(`#template-list [data-template-id="${templateId}"]`);
    if (selectedItem) {
        selectedItem.classList.add('selected');
    }
    
    // Enable generate button and reset text
    document.getElementById('generate-btn').disabled = false;
    document.getElementById('generate-btn').textContent = 'Generate Documents';
}

async function loadPropertySummary() {
    try {
        // Fetch properties directly using existing single-property API
        const properties = await Promise.all(
            selectedProperties.map(async (p_id) => {
                const response = await fetch(`/api/properties/${p_id}`);
                if (!response.ok) {
                    console.error(`Failed to fetch property ${p_id}`);
                    return null;
                }
                const data = await response.json();
                return {
                    p_id: data.p_id,
                    or_id: data.or_id,
                    p_apn: data.p_apn,
                    p_county: data.p_county,
                    p_state: data.p_state,
                    or_name: data.or_name,
                    p_status: data.p_status
                };
            })
        );
        
        // Filter out any failed fetches
        const validProperties = properties.filter(p => p !== null);
        
        console.log(`Loaded ${validProperties.length} properties for display`);
        
        // Group by owner
        const owners = {};
        validProperties.forEach(prop => {
            if (!owners[prop.or_id]) {
                owners[prop.or_id] = {
                    name: prop.or_name || 'Unknown Owner',
                    properties: []
                };
            }
            owners[prop.or_id].properties.push(prop);
        });
        
        // Render summary
        const summaryDiv = document.getElementById('property-summary');
        summaryDiv.innerHTML = '';
        
        if (validProperties.length === 0) {
            summaryDiv.innerHTML = '<p style="color: red;">No properties found</p>';
            return;
        }
        
        Object.entries(owners).forEach(([or_id, owner]) => {
            const ownerDiv = document.createElement('div');
            ownerDiv.className = 'owner-group';
            
            const header = document.createElement('h4');
            header.textContent = `${owner.name} (${owner.properties.length} property${owner.properties.length > 1 ? 'ies' : ''})`;
            ownerDiv.appendChild(header);
            
            const list = document.createElement('ul');
            owner.properties.forEach(prop => {
                const item = document.createElement('li');
                item.textContent = `APN: ${prop.p_apn} | ${prop.p_county}, ${prop.p_state} | Status: ${prop.p_status || 'N/A'}`;
                list.appendChild(item);
            });
            
            ownerDiv.appendChild(list);
            summaryDiv.appendChild(ownerDiv);
        });
        
    } catch (error) {
        console.error('Error loading property summary:', error);
        document.getElementById('property-summary').innerHTML = '<p style="color: red;">Error loading properties</p>';
    }
}

// Change Status Popup Functions
function closePopup() {
    document.getElementById('popup-overlay').style.display = 'none';
    document.getElementById('change-status-popup').style.display = 'none';
}

async function openChangeStatusPopup() {
    console.log('Opening change status popup for', selectedProperties.length, 'properties');
    
    if (selectedProperties.length === 0) {
        alert('No records selected for status change');
        return;
    }
    
    const popup = document.getElementById('change-status-popup');
    const dropdown = document.getElementById('status-dropdown');
    const recordCount = document.getElementById('status-record-count');
    
    // Clear existing options except the first one
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
        
        recordCount.textContent = selectedProperties.length;
        
        document.getElementById('popup-overlay').style.display = 'block';
        popup.style.display = 'block';
        
        // Setup confirm button handler
        document.getElementById('confirm-status-change').onclick = async function() {
            const newStatusId = dropdown.value;
            if (!newStatusId) {
                alert('Please select a status');
                return;
            }
            
            await changeStatusForRecords(selectedProperties, parseInt(newStatusId));
        };
        
    } catch (error) {
        console.error('Error loading statuses:', error);
        alert('Error loading status options');
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
            const result = await response.json();
            alert(`Status updated successfully for ${result.updated} records!`);
            closePopup();
            // Optionally refresh the page or go back to dashboard
            // window.location.href = '/'; 
        } else {
            const errorData = await response.json();
            alert('Error updating status: ' + (errorData.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error changing status:', error);
        alert('Error updating status: ' + error.message);
    }
}