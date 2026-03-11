// Global state
let mode = window.MODE; // 'new', 'edit', or 'copy'
let pId = window.P_ID;
let propertyData = {};
let statuses = [];
let allTags = [];
let selectedTags = [];
let mailImages = []; // Array of {path, name, slot}
let hasUnsavedChanges = false;

// States list for dropdowns
const STATES = [
    // US States
    {value: 'AL', label: 'Alabama'}, {value: 'AK', label: 'Alaska'}, {value: 'AZ', label: 'Arizona'},
    {value: 'AR', label: 'Arkansas'}, {value: 'CA', label: 'California'}, {value: 'CO', label: 'Colorado'},
    {value: 'CT', label: 'Connecticut'}, {value: 'DE', label: 'Delaware'}, {value: 'DC', label: 'District Of Columbia'},
    {value: 'FL', label: 'Florida'}, {value: 'GA', label: 'Georgia'}, {value: 'HI', label: 'Hawaii'},
    {value: 'ID', label: 'Idaho'}, {value: 'IL', label: 'Illinois'}, {value: 'IN', label: 'Indiana'},
    {value: 'IA', label: 'Iowa'}, {value: 'KS', label: 'Kansas'}, {value: 'KY', label: 'Kentucky'},
    {value: 'LA', label: 'Louisiana'}, {value: 'ME', label: 'Maine'}, {value: 'MD', label: 'Maryland'},
    {value: 'MA', label: 'Massachusetts'}, {value: 'MI', label: 'Michigan'}, {value: 'MN', label: 'Minnesota'},
    {value: 'MS', label: 'Mississippi'}, {value: 'MO', label: 'Missouri'}, {value: 'MT', label: 'Montana'},
    {value: 'NE', label: 'Nebraska'}, {value: 'NV', label: 'Nevada'}, {value: 'NH', label: 'New Hampshire'},
    {value: 'NJ', label: 'New Jersey'}, {value: 'NM', label: 'New Mexico'}, {value: 'NY', label: 'New York'},
    {value: 'NC', label: 'North Carolina'}, {value: 'ND', label: 'North Dakota'}, {value: 'OH', label: 'Ohio'},
    {value: 'OK', label: 'Oklahoma'}, {value: 'OR', label: 'Oregon'}, {value: 'PA', label: 'Pennsylvania'},
    {value: 'PR', label: 'Puerto Rico'}, {value: 'RI', label: 'Rhode Island'}, {value: 'SC', label: 'South Carolina'},
    {value: 'SD', label: 'South Dakota'}, {value: 'TN', label: 'Tennessee'}, {value: 'TX', label: 'Texas'},
    {value: 'UT', label: 'Utah'}, {value: 'VT', label: 'Vermont'}, {value: 'VA', label: 'Virginia'},
    {value: 'WA', label: 'Washington'}, {value: 'WV', label: 'West Virginia'}, {value: 'WI', label: 'Wisconsin'},
    {value: 'WY', label: 'Wyoming'},
    // US Territories
    {value: 'AS', label: 'American Samoa'}, {value: 'GU', label: 'Guam'},
    {value: 'MP', label: 'Northern Mariana Islands'}, {value: 'VI', label: 'U.S. Virgin Islands'},
    {value: 'UM', label: 'U.S. Minor Outlying Islands'},
    // US Military
    {value: 'AE', label: 'Armed Forces Europe, Africa, Middle East, Canada'},
    {value: 'AP', label: 'Armed Forces Pacific'}, {value: 'AA', label: 'Armed Forces Americas'},
    // Canada Provinces
    {value: 'AB', label: 'Alberta'}, {value: 'BC', label: 'British Columbia'},
    {value: 'MB', label: 'Manitoba'}, {value: 'NB', label: 'New Brunswick'},
    {value: 'NL', label: 'Newfoundland and Labrador'}, {value: 'NS', label: 'Nova Scotia'},
    {value: 'ON', label: 'Ontario'}, {value: 'PE', label: 'Prince Edward Island'},
    {value: 'QC', label: 'Quebec'}, {value: 'SK', label: 'Saskatchewan'},
    // Canada Territories
    {value: 'NT', label: 'Northwest Territories'}, {value: 'YT', label: 'Yukon'},
    {value: 'NU', label: 'Nunavut'}
];

// Initialize
document.addEventListener('DOMContentLoaded', function() {
    console.log('Property Record page loaded. Mode:', mode, 'P_ID:', pId);
    setupEventListeners();
    loadStatuses();
    loadTags();
    populateStateDropdowns();
    
    if (mode === 'edit') {
        loadPropertyData();
    } else {
        // New record - check if copying from existing
        const urlParams = new URLSearchParams(window.location.search);
        const copyFromId = urlParams.get('copy_from');
        
        if (copyFromId) {
            loadPropertyData(parseInt(copyFromId));
        } else {
            // Truly blank new record
            document.getElementById('p_id').textContent = '(Will be auto-assigned)';
            setTitle('Add New Property');
            
            const now = new Date().toLocaleString('en-US', {
                month: '2-digit', day: '2-digit', year: 'numeric',
                hour: '2-digit', minute: '2-digit', hour12: true
            }).replace(',', '');
            document.getElementById('p_create_time').textContent = now;
            document.getElementById('p_last_updated').textContent = now;
            document.getElementById('p_status_last_updated').textContent = now;
            
            setTimeout(() => {
                const statusSelect = document.getElementById('p_status');
                if (statusSelect.options.length > 0) {
                    statusSelect.value = 1;
                }
            }, 100);
            
            document.querySelector('label[for="p_id"]').style.display = 'none';
            document.getElementById('p_agent_phone').value = '';
        }
    }
});

function formatCurrency(value) {
    if (!value || isNaN(value)) return '';
    return parseFloat(value).toFixed(2);
}

function parseCurrency(value) {
    if (!value || value === '') return null;
    // Remove $ and commas, then parse as float
    const cleaned = String(value).replace(/[$,]/g, '');
    const num = parseFloat(cleaned);
    return isNaN(num) ? null : num;
}

// Event Listeners
function setupEventListeners() {
    // Prevent Enter from submitting
    document.getElementById('property-form').addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && e.target.tagName !== 'TEXTAREA') {
            e.preventDefault();
            const inputs = Array.from(document.querySelectorAll('input, select, textarea'));
            const currentIndex = inputs.indexOf(e.target);
            if (currentIndex < inputs.length - 1) {
                inputs[currentIndex + 1].focus();
            }
        }
    });
    
    // Form change tracking
    document.getElementById('property-form').addEventListener('input', () => {
        hasUnsavedChanges = true;
    });

    // CRITICAL: Prevent form submission 
    document.getElementById('property-form').addEventListener('submit', (e) => {
        e.preventDefault();
    });
    
    // Header buttons
    document.getElementById('back-btn').addEventListener('click', () => handleBack());
    document.getElementById('add-additional-btn').addEventListener('click', () => handleAddAdditional());
    document.getElementById('update-btn').addEventListener('click', (e) => {
        e.preventDefault();
        saveProperty();
    });
	
    // Map button - Opens in new tab with address OR lat/long support
    document.getElementById('map-btn').addEventListener('click', () => {
        const address = document.getElementById('p_address').value.trim();
        const city = document.getElementById('p_city').value.trim();
        const state = document.getElementById('p_state').value.trim();
        const zip = document.getElementById('p_zip').value.trim();

        // STRICT lat/long pattern: exactly 6 decimals, negative longitude
        // Format: ##.######, -##.######
        const latLongPattern = /^\d+\.\d{6},\s*-\d+\.\d{6}$/;
        
        let mapUrl;
        
        // PRIORITY 1: Use lat/long if present (most precise)
        if (latLongPattern.test(address)) {
            console.log('Using lat/long coordinates:', address);
            mapUrl = `https://www.google.com/maps?q=${encodeURIComponent(address)}`;
        } 
        // PRIORITY 2: Use full address
        else if (city || zip) {
            const fullAddress = `${address}, ${city}, ${state} ${zip}`.trim();
            if (!fullAddress || fullAddress === ', , ') {
                alert('Please enter a complete address (street, city, state, and zip)');
                return;
            }
            console.log('Using full address:', fullAddress);
            mapUrl = `https://www.google.com/maps?q=${encodeURIComponent(fullAddress)}`;
        } 
        // Invalid input
        else {
            alert('Please enter either:\n1) Latitude/longitude coordinates in format: 35.387100, -83.261304\n2) A complete address (with city/state/zip)');
            return;
        }
        
        // Open in new tab
        window.open(mapUrl, '_blank');
        console.log('Opening map URL:', mapUrl);
    });
    


    // Footer buttons
    document.getElementById('back-btn-footer').addEventListener('click', () => handleBack());
    document.getElementById('add-additional-footer').addEventListener('click', () => handleAddAdditional());
    document.getElementById('update-footer').addEventListener('click', (e) => {
        e.preventDefault();
        saveProperty();
    });

// Add document-level event delegation for delete buttons
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('btn-delete-item')) {
        const id = e.target.dataset.id;
        const type = e.target.dataset.type;
        
        if (confirm(`Delete this ${type.slice(0, -1)}?`)) {
            let url = '';
            if (type === 'photos') url = `/api/properties/${pId}/photos/${id}`;
            else if (type === 'documents') url = `/api/properties/${pId}/documents/${id}`;
            else if (type === 'links') url = `/api/properties/${pId}/links/${id}`;
            
            fetch(url, { method: 'DELETE' })
                .then(response => response.json())
                .then(result => {
                    if (result.success) {
                        loadPropertyData();
                        showMessage('Deleted successfully', 'success');
                    } else {
                        alert('Error: ' + result.error);
                    }
                });
        }
    }
});
    
// Toggle buttons - FIXED for Other Owners
document.querySelectorAll('.btn-toggle').forEach(btn => {
    btn.addEventListener('click', (e) => {
        const button = e.target;
        const value = button.dataset.value;
        const group = button.parentElement;
        
        // Remove active from siblings
        group.querySelectorAll('.btn-toggle').forEach(b => b.classList.remove('active'));
        
        // Add active to clicked
        button.classList.add('active');
        
        // Update hidden input
        const hiddenInput = group.nextElementSibling;
        if (hiddenInput && hiddenInput.tagName === 'INPUT') {
            hiddenInput.value = value;
        }
        
        // Special handling for Other Owners - FIXED
        if (group.parentElement.classList.contains('other-owners')) {
            if (value === '1') {
                document.getElementById('additional-owners').style.display = 'block';
                document.getElementById('o_other_owners').value = 1;
            } else {
                document.getElementById('additional-owners').style.display = 'none';
                document.getElementById('o_other_owners').value = 0;
            }
        }
        
        hasUnsavedChanges = true;
    });
});
    
    // Owner type toggle
    document.querySelectorAll('[data-value="Individual"], [data-value="Company"]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            toggleOwnerType(e.target.dataset.value);
            hasUnsavedChanges = true;
        });
    });
    
    // Improvements checkboxes
    document.querySelectorAll('.improvements-grid input[type="checkbox"]').forEach(cb => {
        cb.addEventListener('change', updateImprovementsField);
    });
    
    // Conversions
    document.getElementById('p_sqft').addEventListener('blur', convertSqftToAcres);
    document.getElementById('p_acres').addEventListener('blur', convertAcresToSqft);
    
    // Profit calculation
    ['p_purchase_amount', 'p_purchase_closing_costs', 'p_sold_amount', 'p_sold_closing_costs'].forEach(id => {
        document.getElementById(id).addEventListener('input', calculateProfit);
    });
    
    // Upload buttons
    document.getElementById('upload-photo-btn').addEventListener('click', () => {
        document.getElementById('photo-upload').click();
    });
    document.getElementById('photo-upload').addEventListener('change', uploadPhoto);
	
		document.getElementById('upload-mail-image-btn').addEventListener('click', () => {
        if (mailImages.length >= 2) {
            alert('Maximum 2 mail images allowed');
            return;
        }
        document.getElementById('mail-image-upload').click();
    });
    document.getElementById('mail-image-upload').addEventListener('change', uploadMailImage);
    
    document.getElementById('upload-doc-btn').addEventListener('click', () => {
        document.getElementById('doc-upload').click();
    });
    document.getElementById('doc-upload').addEventListener('change', uploadDocument);
    
    // Add link button
    document.getElementById('add-link-btn').addEventListener('click', addLink);
    
    // Tag dropdown
    document.getElementById('tag-dropdown').addEventListener('change', addTagToProperty);
}

// Data Loading
async function loadStatuses() {
    const response = await fetch('/api/statuses');
    const data = await response.json();
    statuses = data.statuses;
    
    const select = document.getElementById('p_status');
    select.innerHTML = ''; // Clear existing
    
    statuses.forEach(status => {
        const option = document.createElement('option');
        option.value = status.status_id;
        option.textContent = status.s_status;
        select.appendChild(option);
    });
}

async function loadTags() {
    const response = await fetch('/api/tags');
    const data = await response.json();
    allTags = data.tags;
    
    const dropdown = document.getElementById('tag-dropdown');
    dropdown.innerHTML = '<option value="">Add a tag...</option>'; // Clear and add default
    
    allTags.forEach(tag => {
        const option = document.createElement('option');
        option.value = tag.tag_id;
        option.textContent = `${tag.tag_id}: ${tag.tag_name}`;
        dropdown.appendChild(option);
    });
}

function populateStateDropdowns() {
    const mailingSelect = document.getElementById('or_m_state');
    const propertySelect = document.getElementById('p_state');
    
    STATES.forEach(state => {
        const option1 = document.createElement('option');
        option1.value = state.value;
        option1.textContent = state.label;
        mailingSelect.appendChild(option1);
        
        const option2 = document.createElement('option');
        option2.value = state.value;
        option2.textContent = state.label;
        propertySelect.appendChild(option2);
    });
}

async function loadPropertyData(copyFromId = null) {
    // MODE 1: New blank record
    if (mode === 'new' && !copyFromId) {
        document.getElementById('p_id').textContent = '(Will be auto-assigned)';
        setTitle('Add New Property');
        
        const now = new Date().toLocaleString('en-US', {
            month: '2-digit', day: '2-digit', year: 'numeric',
            hour: '2-digit', minute: '2-digit', hour12: true
        }).replace(',', '');
        document.getElementById('p_create_time').textContent = now;
        document.getElementById('p_last_updated').textContent = now;
        document.getElementById('p_status_last_updated').textContent = now;
        
        setTimeout(() => {
            const statusSelect = document.getElementById('p_status');
            if (statusSelect.options.length > 0) {
                statusSelect.value = 1;
            }
        }, 100);
        
        document.querySelector('label[for="p_id"]').style.display = 'none';
        document.getElementById('p_agent_phone').value = '';
        return;
    }
    
    // MODE 2: Fetch data for either edit or copy
    const idToLoad = copyFromId || pId;
    const response = await fetch(`/api/properties/${idToLoad}`);
    const fullData = await response.json();
    
    // Extract nested arrays (these are handled separately)
    const { photos = [], documents = [], links = [], tags = [], ...data } = fullData;
    
    if (mode === 'new' && copyFromId) {
        // Add Additional Property - copy ONLY these specific fields
        propertyData = {
            // Caller information (always copy)
            or_fname: data.or_fname,
            or_lname: data.or_lname,
            or_email: data.or_email,
            or_phone: data.or_phone,
            or_fax: data.or_fax,
            
            // Primary owner information
            o_type: data.o_type,
            o_fname: data.o_fname,
            o_lname: data.o_lname,
            o_company: data.o_company,
            
            // Mailing address
            or_m_address: data.or_m_address,
            or_m_address2: data.or_m_address2,
            or_m_city: data.or_m_city,
            or_m_state: data.or_m_state,
            or_m_zip: data.or_m_zip,
            
            // Do NOT copy additional owners
            o_other_owners: 0
        };
        
        setTitle('Add Additional Property');
        
        // Hide additional owners section
        document.getElementById('additional-owners').style.display = 'none';
        
        // Set current timestamps
        const now = new Date().toLocaleString('en-US', {
            month: '2-digit', day: '2-digit', year: 'numeric',
            hour: '2-digit', minute: '2-digit', hour12: true
        }).replace(',', '');
        document.getElementById('p_create_time').textContent = now;
        document.getElementById('p_last_updated').textContent = now;
        document.getElementById('p_status_last_updated').textContent = now;
        
        // Render empty uploads for new record
        renderUploads('photos', []);
        renderUploads('documents', []);
        renderUploads('links', []);
        
    } else {
        // MODE 3: Edit existing record
        propertyData = data;
        setTitle('Edit Property');
        document.getElementById('p_id').textContent = propertyData.p_id;
        
        // Populate timestamps
        document.getElementById('p_create_time').textContent = propertyData.p_create_time || '';
        document.getElementById('p_last_updated').textContent = propertyData.p_last_updated || '';
        document.getElementById('p_status_last_updated').textContent = propertyData.p_status_last_updated || '';
        
        // Load tags and uploads
        selectedTags = tags;
        renderSelectedTags();
        renderUploads('photos', photos);
        renderUploads('documents', documents);
        renderUploads('links', links);
		
		// Load mail images
		mailImages = [];
		if (data.p_mail_image_1) {
			mailImages.push({
				path: data.p_mail_image_1,
				name: data.p_mail_image_1.split('/').pop(),
				slot: 1
			});
		}
		if (data.p_mail_image_2) {
			mailImages.push({
				path: data.p_mail_image_2,
				name: data.p_mail_image_2.split('/').pop(),
				slot: 2
			});
		}
		renderMailImages();
		
    }
    
    // Populate form fields (only flat fields, no nested arrays)
    Object.keys(propertyData).forEach(key => {
        const element = document.getElementById(key);
        if (element && propertyData[key] !== null && propertyData[key] !== undefined) {
            let value = propertyData[key];
            
            if (element.type === 'number' && parseFloat(value) === -0.01) {
                value = '';
            }
            
            if (element.tagName === 'SELECT') {
                element.value = value;
            } else if (element.type === 'checkbox') {
                element.checked = value == 1;
            } else if (element.type === 'date' && value) {
                element.value = value.split(' ')[0];
            } else {
                element.value = value;
            }
        }
    });
    
	// Load new transaction fields (dates and currency formatting)
    if (propertyData.p_last_sold_date) document.getElementById('p_last_sold_date').value = propertyData.p_last_sold_date;
    if (propertyData.p_last_sold_amount) document.getElementById('p_last_sold_amount').value = formatCurrency(propertyData.p_last_sold_amount);
    if (propertyData.p_last_transaction_date) document.getElementById('p_last_transaction_date').value = propertyData.p_last_transaction_date;
    if (propertyData.p_last_transaction_doc_type) document.getElementById('p_last_transaction_doc_type').value = propertyData.p_last_transaction_doc_type;
	
    // Handle improvements
    if (propertyData.p_improvements) {
        const improvements = propertyData.p_improvements.split('|');
        improvements.forEach(imp => {
            const trimmed = imp.trim();
            if (trimmed) {
                const checkbox = document.querySelector(`input[value="${trimmed}"]`);
                if (checkbox) checkbox.checked = true;
            }
        });
    }
    
    // Set owner type toggle
    if (propertyData.o_type) {
        toggleOwnerType(propertyData.o_type);
    }
    
    // Set status dropdown
    const setStatusValue = () => {
        const statusSelect = document.getElementById('p_status');
        if (statusSelect && statusSelect.options.length > 0) {
            if (mode === 'new' && copyFromId) {
                statusSelect.value = 1; // Prospect
            } else if (propertyData.p_status_id) {
                statusSelect.value = propertyData.p_status_id;
            }
        } else {
            setTimeout(setStatusValue, 50);
        }
    };
    setStatusValue();
    
    // Auto-detect and show additional owners if data exists
    const hasAdditionalOwners = propertyData.o_2fname || propertyData.o_2lname || 
                           propertyData.o_3fname || propertyData.o_3lname ||
                           propertyData.o_4fname || propertyData.o_4lname ||
                           propertyData.o_5fname || propertyData.o_5lname;
    
    if (hasAdditionalOwners) {
        document.getElementById('o_other_owners').value = 1;
        document.getElementById('additional-owners').style.display = 'block';
        const buttonGroup = document.querySelector('.other-owners .button-group');
        if (buttonGroup) {
            buttonGroup.querySelector('[data-value="1"]').classList.add('active');
            buttonGroup.querySelector('[data-value="0"]').classList.remove('active');
        }
    } else if (mode !== 'new' || !copyFromId) {
        document.getElementById('additional-owners').style.display = 'none';
    }
    
    // Final UI updates
    generateComparables();
    calculateProfit();
}

// Helper Functions
function setTitle(title) {
    document.getElementById('page-title').textContent = title + ' - Valiant Land';
    document.getElementById('record-title').textContent = title;
}

function toggleOwnerType(type) {
    const individualBtn = document.querySelector('[data-value="Individual"]');
    const companyBtn = document.querySelector('[data-value="Company"]');
    const companyFields = document.getElementById('company-fields');
    const ownerNames = document.getElementById('owner-names');
    
    if (type === 'Company') {
        companyBtn.classList.add('active');
        individualBtn.classList.remove('active');
        companyFields.style.display = 'block';
        ownerNames.style.display = 'none';
    } else {
        individualBtn.classList.add('active');
        companyBtn.classList.remove('active');
        companyFields.style.display = 'none';
        ownerNames.style.display = 'flex';
    }
    
    document.getElementById('o_type').value = type;
    hasUnsavedChanges = true;
	
	// Toggle required attributes based on owner type
	const isIndividual = type !== 'Company';
	document.getElementById('or_fname').required = isIndividual;
	document.getElementById('or_lname').required = isIndividual;

	// Also toggle visual indicators if you have asterisks
	const fnameLabel = document.querySelector('label[for="or_fname"]');
	const lnameLabel = document.querySelector('label[for="or_lname"]');
	if (isIndividual) {
		fnameLabel.innerHTML = 'First Name:*';
		lnameLabel.innerHTML = 'Last Name:*';
	} else {
		fnameLabel.innerHTML = 'First Name:';
		lnameLabel.innerHTML = 'Last Name:';
	}
}

function handleToggle(e) {
    const button = e.target;
    const value = button.dataset.value;
    const group = button.parentElement;
    
    // Remove active from siblings
    group.querySelectorAll('.btn-toggle').forEach(btn => btn.classList.remove('active'));
    
    // Add active to clicked
    button.classList.add('active');
    
    // Update hidden input
    const hiddenInput = group.nextElementSibling;
    if (hiddenInput && hiddenInput.tagName === 'INPUT') {
        hiddenInput.value = value;
    }
    
    hasUnsavedChanges = true;
}

function updateImprovementsField() {
    const checkboxes = document.querySelectorAll('.improvements-grid input[type="checkbox"]:checked');
    const otherText = document.getElementById('p_improvements_other').value;
    
    const values = Array.from(checkboxes).map(cb => cb.value);
    if (otherText) values.push(otherText);
    
    document.getElementById('p_improvements').value = values.join('|');
    hasUnsavedChanges = true;
}

function convertSqftToAcres() {
    const sqft = parseFloat(document.getElementById('p_sqft').value);
    if (sqft && !isNaN(sqft)) {
        const acres = sqft / 43560;
        document.getElementById('p_acres').value = acres.toFixed(4);
        hasUnsavedChanges = true;
    }
}

function convertAcresToSqft() {
    const acres = parseFloat(document.getElementById('p_acres').value);
    if (acres && !isNaN(acres)) {
        const sqft = acres * 43560;
        document.getElementById('p_sqft').value = Math.round(sqft);
        hasUnsavedChanges = true;
    }
}

function calculateProfit() {
    const purchase = parseFloat(document.getElementById('p_purchase_amount').value) || 0;
    const purchaseClosing = parseFloat(document.getElementById('p_purchase_closing_costs').value) || 0;
    const sold = parseFloat(document.getElementById('p_sold_amount').value) || 0;
    const soldClosing = parseFloat(document.getElementById('p_sold_closing_costs').value) || 0;
    
    const profit = sold - (purchase + purchaseClosing + soldClosing);
    document.getElementById('p_profit').value = profit.toFixed(2);
}

function renderSelectedTags() {
    const container = document.getElementById('selected-tags');
    container.innerHTML = '';
    
    selectedTags.forEach(tag => {
        const tagEl = document.createElement('div');
        tagEl.className = 'tag-chip';
        tagEl.innerHTML = `
            ${tag.tag_name}
            <span class="tag-remove" data-tag-id="${tag.tag_id}">&times;</span>
        `;
        container.appendChild(tagEl);
    });
    
    document.querySelectorAll('.tag-remove').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const tagId = parseInt(e.target.dataset.tagId);
            selectedTags = selectedTags.filter(t => t.tag_id !== tagId);
            renderSelectedTags();
            hasUnsavedChanges = true;
        });
    });
}

function addTagToProperty() {
    const dropdown = document.getElementById('tag-dropdown');
    const tagId = parseInt(dropdown.value);
    
    if (!tagId) return;
    
    const tag = allTags.find(t => t.tag_id === tagId);
    if (tag && !selectedTags.find(t => t.tag_id === tagId)) {
        selectedTags.push(tag);
        renderSelectedTags();
        dropdown.value = '';
        hasUnsavedChanges = true;
    }
}

// Upload Functions
function uploadPhoto(e) {
    const file = e.target.files[0];
    if (!file) return;
    
    if (!pId) {
        alert('Please save the property record first before uploading files');
        return;
    }
    
    const formData = new FormData();
    formData.append('file', file);
    
    fetch(`/api/properties/${pId}/photos`, {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            loadPropertyData();
            showMessage('Photo uploaded successfully!', 'success');
            document.getElementById('photo-upload').value = '';
        }
    });
}

function uploadMailImage(e) {
    const file = e.target.files[0];
    if (!file) return;
    
    if (!pId) {
        alert('Please save the property record first before uploading files');
        return;
    }
    
    if (mailImages.length >= 2) {
        alert('Maximum 2 mail images allowed');
        return;
    }
    
    const formData = new FormData();
    formData.append('file', file);
    
    fetch(`/api/properties/${pId}/mail-images`, {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            mailImages.push({
                path: result.file_path,
                name: result.file_name,
                slot: result.slot
            });
            renderMailImages();
            showMessage('Mail image uploaded successfully!', 'success');
            document.getElementById('mail-image-upload').value = '';
        } else {
            alert('Error: ' + result.error);
        }
    })
    .catch(error => {
        console.error('Upload error:', error);
        alert('Error uploading mail image');
    });
}

function renderMailImages() {
    const container = document.getElementById('mail-images-list');
    container.innerHTML = '';
    
    if (mailImages.length === 0) {
        container.innerHTML = '<div style="padding: 10px; color: #666;">No mail images</div>';
        return;
    }
    
    mailImages.forEach((img, index) => {
        const div = document.createElement('div');
        div.className = 'upload-item';
        const url = window.STATIC_URL + (img.path || '').replace(/\\/g, '/');
        
        let controls = '';
        if (mailImages.length === 1) {
            controls = '<span style="margin-left: 10px; color: #666; font-size: 0.9em;">Mail Image 1</span>';
        } else {
            const isImage1 = index === 0;
            controls = `
                <label style="margin-left: 10px; font-size: 0.9em; cursor: pointer;">
                    <input type="radio" name="mail-image-designation" value="${index}" ${isImage1 ? 'checked' : ''} onchange="swapMailImages(${index})">
                    Mail Image 1
                </label>
            `;
        }
        
        div.innerHTML = `
            <a href="${url}" target="_blank">${img.name}</a>
            ${controls}
            <button class="btn-delete-item" onclick="deleteMailImage(${index})" style="margin-left: auto;">×</button>
        `;
        container.appendChild(div);
    });
}

function swapMailImages(selectedIndex) {
    if (selectedIndex === 1 && mailImages.length === 2) {
        const temp = mailImages[0];
        mailImages[0] = mailImages[1];
        mailImages[1] = temp;
        mailImages[0].slot = 1;
        mailImages[1].slot = 2;
        renderMailImages();
        hasUnsavedChanges = true;
    }
}

function deleteMailImage(index) {
    if (!confirm('Delete this mail image?')) return;
    
    const img = mailImages[index];
    
    fetch(`/api/properties/${pId}/mail-images`, {
        method: 'DELETE',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({slot: img.slot})
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            loadPropertyData();
            showMessage('Mail image deleted', 'success');
        } else {
            alert('Error: ' + (result.error || 'Failed to delete mail image'));
        }
    })
    .catch(error => {
        console.error('Delete error:', error);
        alert('Error deleting mail image');
    });
}

function uploadDocument(e) {
    const file = e.target.files[0];
    if (!file) return;
    
    if (!pId) {
        alert('Please save the property record first before uploading files');
        return;
    }
    
    const formData = new FormData();
    formData.append('file', file);
    
    fetch(`/api/properties/${pId}/documents`, {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            loadPropertyData();
            showMessage('Document uploaded successfully!', 'success');
            document.getElementById('doc-upload').value = '';
        }
    });
}
function addLink() {
    const url = document.getElementById('link-url').value;
    const description = document.getElementById('link-desc').value;
    
    if (!url) {
        alert('Please enter a URL');
        return;
    }
    
    fetch(`/api/properties/${pId}/links`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({url, description})
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            document.getElementById('link-url').value = '';
            document.getElementById('link-desc').value = '';
            loadPropertyData();
            showMessage('Link added successfully!', 'success');
        }
    });
}

function renderUploads(type, items) {
    console.log(`DEBUG: Rendering ${type}`, items, 'Type:', typeof items); // Debug log
    
    const container = document.getElementById(`${type}-list`);
    if (!container) {
        console.error(`ERROR: Container #${type}-list not found!`); // Alert if ID is wrong
        return;
    }
    
    container.innerHTML = '';
    
    // Ensure items is actually an array
    if (!Array.isArray(items)) {
        console.warn(`WARN: ${type} is not an array:`, items); // Debug wrong type
        container.innerHTML = '<div style="padding: 10px; color: red;">Error: Invalid data format</div>';
        return;
    }
    
    if (items.length === 0) {
        container.innerHTML = '<div style="padding: 10px; color: #666;">No items</div>';
        return;
    }
    
    items.forEach((item, index) => {
        const div = document.createElement('div');
        div.className = 'upload-item';
        
        // Debug each item
        console.log(`DEBUG ${type}[${index}]:`, item);
        
        let url = '';
        let text = '';
        let itemId = 'unknown';
        
        if (type === 'photos') {
            url = window.STATIC_URL + (item.file_path || '');
            text = item.file_name || 'Photo';
            itemId = item.photo_id || index;
        } else if (type === 'documents') {
            url = window.STATIC_URL + (item.file_path || '');
            text = item.file_name || 'Document';
            itemId = item.doc_id || index;
        } else if (type === 'links') {
            // Explicit safety checks for links
            url = item.url || '#';
            text = item.description || item.url || 'Link';
            itemId = item.link_id || index;
            
            // Validate URL
            if (!item.url) {
                console.error('ERROR: Link missing URL:', item);
                text = 'ERROR: Invalid link';
                url = '#';
            }
        } else {
            console.error('ERROR: Unknown type:', type);
            return;
        }
        
        div.innerHTML = `
            <a href="${url}" target="_blank">${text}</a>
            <button class="btn-delete-item" data-id="${itemId}" data-type="${type}">×</button>
        `;
        container.appendChild(div);
    });
}    

// Comparables URLs
function generateComparables() {
    const p_zip = document.getElementById('p_zip').value.trim();
    const p_sqft = parseFloat(document.getElementById('p_sqft').value) || 0;
    const p_acres = parseFloat(document.getElementById('p_acres').value) || 0;
    
    const container = document.getElementById('comparables-links');
    container.innerHTML = '';
    
    if (!p_zip) return;
    
    // SQFT intervals for calculations
    const sqftIntervals = [1000,2000,3000,4000,5000,7500,10890,21780,43560,87120,217800,435600,871200,2178000,4356000];
    const redfinSqftIntervals = [2000,4500,6500,8000,9500];
    const redfinAcreIntervals = [.25, .5, 1, 2, 3, 4, 5, 10, 20, 40, 100];
    
    // Helper: Find min/max in interval list
    function getMinMax(value, intervals) {
        let min = 0;
        let max = intervals[intervals.length - 1];
        
        for (let i = 0; i < intervals.length; i++) {
            if (value <= intervals[i]) {
                min = i > 0 ? intervals[i-1] : 0;
                max = intervals[i];
                break;
            }
        }
        return { min, max };
    }
    
    // 1. Zillow.com
    let minsqft = 0;
    let maxsqft = 0;
    
    for (let i = 0; i < sqftIntervals.length; i++) {
        if (p_sqft <= sqftIntervals[i]) {
            minsqft = i > 0 ? sqftIntervals[i-1] : 0;
            maxsqft = sqftIntervals[i];
            break;
        }
    }
    if (maxsqft === 0) minsqft = sqftIntervals[sqftIntervals.length - 1];
    
    const zillowUrl = `https://www.zillow.com/homes/recently_sold/${p_zip}/land_type/1_rs/6m_days/${minsqft}-${maxsqft}_lot`;
    addComparableLink('Zillow.com', zillowUrl);
    
    // 2. Realtor.com
    const realtorMinMax = getMinMax(p_sqft, sqftIntervals);
    const realtorUrl = `https://www.realtor.com/realestateandhomes-search/${p_zip}/type-land/show-recently-sold/lot-sqft-${realtorMinMax.min}-${realtorMinMax.max}`;
    addComparableLink('Realtor.com', realtorUrl);
    
    // 3. Trulia.com
    const truliaUrl = `http://www.trulia.com/sold/${p_zip}_zip/price;a_sort/LOT|LAND_type/${p_acres}p_ls`;
    addComparableLink('Trulia.com', truliaUrl);
    
    // 4. Redfin.com
    let redfinMin = '';
    let redfinMax = '';
    
    if (p_acres < 0.25) {
        // Use sqft for small lots
        const minIndex = redfinSqftIntervals.findIndex(val => p_sqft <= val);
        if (minIndex > 0) {
            const minVal = redfinSqftIntervals[minIndex - 1];
            redfinMin = (minVal / 1000) + 'K-sqft';
        } else {
            redfinMin = '1K-sqft';
        }
        
        const maxIndex = redfinSqftIntervals.findIndex(val => p_sqft < val);
        if (maxIndex >= 0) {
            const maxVal = redfinSqftIntervals[maxIndex];
            if (maxVal <= 9500) {
                redfinMax = (maxVal / 1000) + 'K-sqft';
            } else {
                redfinMax = maxVal / 43560 + '-acre';
            }
        } else {
            redfinMax = '2-acre';
        }
    } else {
        // Use acres for larger lots
        const acreMinMax = getMinMax(p_acres, redfinAcreIntervals);
        redfinMin = acreMinMax.min + '-acre';
        redfinMax = acreMinMax.max + '-acre';
        
        if (p_acres > 100) {
            redfinMax = '';
        }
    }
    
    let redfinUrl = `https://www.redfin.com/zipcode/${p_zip}/filter/property-type=land,min-lot-size=${redfinMin}`;
    if (redfinMax) {
        redfinUrl += `,max-lot-size=${redfinMax}`;
    }
    redfinUrl += ',include=sold-6mo';
    addComparableLink('Redfin.com', redfinUrl);
    
    // 5. LandWatch.com
    const acreageRoundup = Math.ceil(p_acres);
    const landwatchUrl = `http://www.landwatch.com/zip-${p_zip}/land/acres-under-${acreageRoundup}/sort-price-acres-low-high`;
    addComparableLink('LandWatch.com', landwatchUrl);
    
    // 6. Lands of America
    const landsofamericaUrl = `https://www.landsofamerica.com/zip-${p_zip}/all-land/is-sold/`;
    addComparableLink('Lands of America', landsofamericaUrl);
    
    function addComparableLink(name, url) {
        const link = document.createElement('a');
        link.href = url;
        link.textContent = name;
        link.target = '_blank';
        link.style.display = 'block';
        link.style.margin = '5px 0';
        container.appendChild(link);
    }
}

// Navigation
function handleBack() {
    if (hasUnsavedChanges) {
        if (confirm('You have unsaved changes. Are you sure you want to leave without saving?')) {
            window.location.href = '/';
        }
    } else {
        window.location.href = '/';
    }
}

function handleAddAdditional() {
    if (confirm('Create additional property for this owner? Current property will be saved first.')) {
        saveProperty(true);
    }
}

function saveProperty(isAddAdditional = false) {
    console.log('Saving property... Mode:', mode);
    console.log('Current o_type value:', document.getElementById('o_type').value);
    
    // CRITICAL FIX: Ensure o_type is set correctly from the toggle
    const activeToggle = document.querySelector('.form-group:has(#o_type) .btn-toggle.active');
    if (activeToggle) {
        const correctType = activeToggle.dataset.value;
        document.getElementById('o_type').value = correctType;
        console.log('Setting o_type from toggle to:', correctType);
    }
    
    // Collect form data
    const formData = {
        tags: selectedTags.map(t => t.tag_id),
        p_last_sold_date: document.getElementById('p_last_sold_date')?.value || null,
        p_last_sold_amount: parseCurrency(document.getElementById('p_last_sold_amount')?.value),
        p_last_transaction_date: document.getElementById('p_last_transaction_date')?.value || null,
        p_last_transaction_doc_type: document.getElementById('p_last_transaction_doc_type')?.value || null,
    };
    
    const inputs = document.querySelectorAll('#property-form input, #property-form select, #property-form textarea');
    inputs.forEach(input => {
        if (input.type === 'checkbox') {
            formData[input.name] = input.checked ? 1 : 0;
        } else if (input.type === 'date') {
            formData[input.name] = input.value;
        } else {
            formData[input.name] = input.value;
        }
    });
    
    // Get improvements
    const improvements = [];
    document.querySelectorAll('.improvements-grid input[type="checkbox"]:checked').forEach(cb => {
        if (cb.value !== 'on') improvements.push(cb.value);
    });
    const otherImp = document.getElementById('p_improvements_other').value;
    if (otherImp) improvements.push(otherImp);
    formData.p_improvements = improvements.join('|');
    
    // Get status ID (default to 1 for "Prospect")
    formData.p_status_id = document.getElementById('p_status').value || 1;
    
    console.log('Final form data to send:', formData);
    
    const url = mode === 'edit' ? `/api/properties/${pId}` : '/api/properties';
    const method = mode === 'edit' ? 'PUT' : 'POST';
    
	// Add mail image paths to form data (array is already ordered by slot)
    if (mailImages.length === 0) {
        formData.p_mail_image_1 = null;
        formData.p_mail_image_2 = null;
    } else if (mailImages.length === 1) {
        formData.p_mail_image_1 = mailImages[0].path;
        formData.p_mail_image_2 = null;
    } else {
        formData.p_mail_image_1 = mailImages[0].path;
        formData.p_mail_image_2 = mailImages[1].path;
    }
	
    fetch(url, {
        method: method,
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(formData)
    })
    .then(response => response.json())
    .then(result => {
        // Handle confirmation response
        if (result.confirm) {
            if (confirm(result.message)) {
                // User confirmed - add to existing owner
                fetch('/api/properties/confirm-create', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        ...formData,
                        owner_id: result.owner_id
                    })
                })
                .then(r => r.json())
                .then(r => {
                    if (r.success) {
                        showMessage('Record added successfully!', 'success');
                        setTimeout(() => window.location.href = `/property/edit/${r.p_id}`, 1500);
                    } else {
                        showMessage('Error: ' + r.error, 'error');
                    }
                });
            }
            return; // Don't proceed with normal flow
        }
        
        // Normal success flow
        if (result.success) {
            showMessage('Record saved successfully!', 'success');
            hasUnsavedChanges = false;
            
            if (isAddAdditional) {
                window.location.href = `/property/new?copy_from=${pId || result.p_id}`;
            } else if (mode === 'new') {
                setTimeout(() => window.location.href = `/property/edit/${result.p_id}`, 1500);
            } else if (!isAddAdditional) {
                setTimeout(() => window.location.href = '/', 1500);
            }
        } else {
            showMessage('Error: ' + result.error, 'error');
        }
    })
    .catch(error => {
        console.error('Save error:', error);
        showMessage('Error saving record', 'error');
    });
}

function showMessage(text, type) {
    const message = document.getElementById('response-message');
    message.textContent = text;
    message.className = `message ${type}`;
    message.style.display = 'block';
    setTimeout(() => message.style.display = 'none', 3000);
}