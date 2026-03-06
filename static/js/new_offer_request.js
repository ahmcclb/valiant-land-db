// Global state
let formData = {};

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log('New Offer Request page loaded');
    setupEventListeners();
    populateStateDropdowns();
    loadExistingProperty();
});

// Event Listeners
function setupEventListeners() {
    // Toggle buttons
    document.querySelectorAll('.btn-toggle').forEach(btn => {
        btn.addEventListener('click', handleToggle);
    });
    
    // Area conversion
    document.getElementById('p_sqft').addEventListener('blur', convertSqftToAcres);
    document.getElementById('p_acres').addEventListener('blur', convertAcresToSqft);
    
    // Conditional sections
    document.querySelector('[data-value="0"]').addEventListener('click', () => {
        document.getElementById('lien-section').style.display = 'block';
    });
    document.querySelector('[data-value="1"]').addEventListener('click', () => {
        document.getElementById('lien-section').style.display = 'none';
    });
    
    // Form submission
    document.getElementById('cancel-btn').addEventListener('click', () => {
        window.location.href = '/';
    });
    
    document.getElementById('offer-form').addEventListener('submit', handleSubmit);
    
    // Prevent Enter from submitting
    document.getElementById('offer-form').addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && e.target.tagName !== 'TEXTAREA') {
            e.preventDefault();
        }
    });
}

// Populate state dropdowns
function populateStateDropdowns() {
    const STATES = [
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
        {value: 'AS', label: 'American Samoa'}, {value: 'GU', label: 'Guam'},
        {value: 'MP', label: 'Northern Mariana Islands'}, {value: 'VI', label: 'U.S. Virgin Islands'},
        {value: 'AB', label: 'Alberta'}, {value: 'BC', label: 'British Columbia'},
        {value: 'MB', label: 'Manitoba'}, {value: 'NB', label: 'New Brunswick'},
        {value: 'NL', label: 'Newfoundland and Labrador'}, {value: 'NS', label: 'Nova Scotia'},
        {value: 'ON', label: 'Ontario'}, {value: 'PE', label: 'Prince Edward Island'},
        {value: 'QC', label: 'Quebec'}, {value: 'SK', label: 'Saskatchewan'},
        {value: 'NT', label: 'Northwest Territories'}, {value: 'YT', label: 'Yukon'},
        {value: 'NU', label: 'Nunavut'}
    ];
    
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

// REPLACE ENTIRE FUNCTION - Forces status to 3 AFTER loading data for edits
function loadExistingProperty() {
    const urlParams = new URLSearchParams(window.location.search);
    const pId = urlParams.get('p_id');
    const prefillApn = urlParams.get('apn');
    
    const statusField = document.getElementById('p_status_id');
    
    if (pId) {
        // EDIT MODE for Offer Request: Load data THEN force to 3
        console.log('EDIT MODE: Loading existing property for offer processing');
        document.getElementById('editing_p_id').value = pId;
        document.getElementById('submit-btn').textContent = 'Update Record';
        
        fetch(`/api/properties/${pId}`)
            .then(response => response.json())
            .then(data => {
                // Load all property/owner data
                Object.keys(data).forEach(key => {
                    const element = document.getElementById(key);
                    if (element && data[key] !== null && data[key] !== undefined) {
                        element.value = data[key];
                    }
                });
                
                // Handle improvements
                if (data.p_improvements) {
                    data.p_improvements.split('|').forEach(imp => {
                        const checkbox = document.querySelector(`input[value="${imp.trim()}"]`);
                        if (checkbox) checkbox.checked = true;
                    });
                }
                
                // Set toggles
                setToggleValue('p_viable', data.p_viable || '1');
                setToggleValue('p_survey', data.p_survey || '0');
                setToggleValue('p_listed', data.p_listed || '0');
                setToggleValue('o_other_owners', data.o_other_owners || '0');
                
                if (data.o_type === 'Company') {
                    setToggleValue('o_type', 'Company');
                    document.getElementById('company-fields').style.display = 'block';
                }
                
                // === CRITICAL BUSINESS LOGIC ===
                // Processing a new offer request ALWAYS moves to Pending Preliminary Research
                statusField.value = '3';
                console.log('Status forced to 3 for offer processing');
            });
    } else {
        // CREATE MODE: Status is already 3 from HTML
        console.log('CREATE MODE: New property with default status=3');
        document.getElementById('editing_p_id').value = '';
        document.getElementById('submit-btn').textContent = 'Create Record';
        
        if (prefillApn) {
            document.getElementById('p_apn').value = prefillApn;
            document.getElementById('p_apn').setAttribute('readonly', true);
        }
    }
}

// Toggle handler
function handleToggle(e) {
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
    
    // Handle conditional sections
    if (group.parentElement.querySelector('h3')?.textContent.includes('free and clear')) {
        document.getElementById('lien-section').style.display = value === '0' ? 'block' : 'none';
    } else if (group.parentElement.querySelector('h3')?.textContent.includes('someone else on title')) {
        document.getElementById('additional-owners').style.display = value === '1' ? 'block' : 'none';
    } else if (group.parentElement.querySelector('h3')?.textContent.includes('Flood Zone')) {
        document.getElementById('flood-description-section').style.display = value === 'Yes' ? 'block' : 'none';
    } else if (group.parentElement.querySelector('h3')?.textContent.includes('listed with a Realtor')) {
        document.getElementById('agent-section').style.display = value === '1' ? 'block' : 'none';
    } else if (group.parentElement.querySelector('h3')?.textContent.includes('owned by a company')) {
        document.getElementById('company-fields').style.display = value === 'Company' ? 'block' : 'none';
    }
}

// Set toggle value programmatically
function setToggleValue(fieldName, value) {
    const toggle = document.querySelector(`input[name="${fieldName}"]`);
    if (toggle) {
        const buttons = toggle.previousElementSibling.querySelectorAll('.btn-toggle');
        buttons.forEach(btn => {
            btn.classList.remove('active');
            if (btn.dataset.value == value) {
                btn.classList.add('active');
            }
        });
    }
}

// Area conversions
function convertSqftToAcres() {
    const sqft = parseFloat(document.getElementById('p_sqft').value);
    if (sqft && !isNaN(sqft)) {
        const acres = sqft / 43560;
        document.getElementById('p_acres').value = acres.toFixed(4);
    }
}

function convertAcresToSqft() {
    const acres = parseFloat(document.getElementById('p_acres').value);
    if (acres && !isNaN(acres)) {
        const sqft = acres * 43560;
        document.getElementById('p_sqft').value = Math.round(sqft);
    }
}

// Form submission
// CLEAN handleSubmit - drop this in
// REPLACE ENTIRE FUNCTION - DO NOT MODIFY
async function handleSubmit(e) {
    e.preventDefault();
    
    const editingId = document.getElementById('editing_p_id').value;
    const isEditMode = editingId !== '';
    
    // === DEBUG: Verify status at submit start ===
    const statusField = document.getElementById('p_status_id');
    console.log('=== SUBMIT START ===');
    console.log('Status field value:', statusField.value);
    console.log('Status field name:', statusField.name);
    console.log('Is in form?', document.getElementById('offer-form').contains(statusField));
    
    // Use FormData - GUARANTEED to capture ALL fields in the form
    const formData = new FormData(document.getElementById('offer-form'));
    const data = {};
    
    // Convert FormData to object
    for (let [key, value] of formData.entries()) {
        if (key === 'tags') continue; // Handle separately
        data[key] = value;
    }
    
    // Handle checkboxes (FormData skips unchecked boxes)
    document.querySelectorAll('#offer-form input[type="checkbox"][name]').forEach(cb => {
        data[cb.name] = cb.checked ? 1 : 0;
    });
    
    // Handle improvements
    const improvements = [];
    document.querySelectorAll('.improvements-grid input[type="checkbox"]:checked').forEach(cb => {
        if (cb.value !== 'on') improvements.push(cb.value);
    });
    const otherImp = document.getElementById('p_improvements_other').value;
    if (otherImp) improvements.push(otherImp);
    data.p_improvements = improvements.join('|');
    
    // === DEBUG: Verify status in final data ===
    console.log('=== SUBMIT DATA ===');
    console.log('Status in final data:', data.p_status_id);
    console.log('Full data:', JSON.stringify(data));
    
    // Validation
    const requiredFields = ['or_fname', 'or_lname', 'o_fname', 'o_lname', 'p_apn', 'p_county', 'or_m_address', 'or_m_city', 'or_m_state'];
    const missing = requiredFields.filter(f => !data[f]);
    
    if (missing.length > 0) {
        alert('Please fill in all required fields: ' + missing.join(', '));
        return;
    }
    
    try {
        const url = isEditMode ? `/api/properties/${editingId}` : '/api/properties';
        const method = isEditMode ? 'PUT' : 'POST';
        
        const response = await fetch(url, {
            method: method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data)
        });
        
        const result = await response.json();
        
        if (!isEditMode && result.confirm) {
            if (confirm(result.message)) {
                const confirmData = { ...data, owner_id: result.owner_id };
                const confirmResponse = await fetch('/api/properties/confirm-create', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(confirmData)
                });
                
                const confirmResult = await confirmResponse.json();
                if (confirmResult.success) {
                    window.location.href = `/property/edit/${confirmResult.p_id}`;
                } else {
                    alert('Error: ' + confirmResult.error);
                }
            }
        } else if (result.success) {
            const redirectId = isEditMode ? editingId : result.p_id;
            window.location.href = `/property/edit/${redirectId}`;
        } else {
            alert('Error: ' + (result.error || 'Unknown error'));
        }
    } catch (error) {
        console.error('Submission error:', error);
        alert('Error submitting form: ' + error.message);
    }
}