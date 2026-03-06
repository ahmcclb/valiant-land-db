let companyData = {};
const STATIC_URL = window.STATIC_URL || '/static/';

document.addEventListener('DOMContentLoaded', function() {
    console.log('Company Management page loaded');
    loadCompanyData();
    setupEventListeners();
});

function setupEventListeners() {
    // Cancel button
    const cancelBtn = document.getElementById('cancel-btn');
    if (cancelBtn) {
        cancelBtn.addEventListener('click', (e) => {
            console.log('Cancel clicked');
            if (confirm('Discard changes and return to dashboard?')) {
                window.location.href = '/';
            }
        });
    }
    
    // Save button
    const form = document.getElementById('company-form');
    if (form) {
        form.addEventListener('submit', (e) => {
            console.log('Save submitted');
            e.preventDefault();
            saveCompany();
        });
    }
    
    // Upload button
    const uploadBtn = document.getElementById('upload-btn');
    const fileInput = document.getElementById('signature-file');
    
    if (uploadBtn && fileInput) {
        uploadBtn.addEventListener('click', () => {
            console.log('Upload button clicked');
            fileInput.click();
        });
        
        fileInput.addEventListener('change', uploadSignature);
    }
}

async function loadCompanyData() {
    try {
        const response = await fetch('/api/company');
        const data = await response.json();
        companyData = data.company;
        
        console.log('Loaded company data:', companyData);
        
        // Populate form fields
        Object.keys(companyData).forEach(key => {
            const input = document.getElementById(key);
            if (input && companyData[key] !== null) {
                input.value = companyData[key];
            }
        });
        
        // Load signature image
        if (companyData.c_sig_path) {
            const img = document.getElementById('signature-preview');
            img.src = STATIC_URL + companyData.c_sig_path;
            img.style.display = 'block';
            document.getElementById('no-signature').style.display = 'none';
        }
        
    } catch (error) {
        console.error('Error loading company data:', error);
        showMessage('Error loading company information', 'error');
    }
}

function saveCompany() {
    console.log('Saving company...');
    
    const formData = {};
    const inputs = document.querySelectorAll('#company-form input[type="text"], #company-form input[type="tel"], #company-form input[type="email"]');
    
    inputs.forEach(input => {
        formData[input.name] = input.value;
    });
    
    console.log('Form data to save:', formData);
    
    fetch('/api/company', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(formData)
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            showMessage('Company information saved successfully!', 'success');
            setTimeout(() => window.location.href = '/', 1500);
        } else {
            showMessage('Error saving company information', 'error');
        }
    })
    .catch(error => {
        console.error('Save error:', error);
        showMessage('Error saving company information', 'error');
    });
}

function uploadSignature(e) {
    const file = e.target.files[0];
    if (!file) return;
    
    console.log('Uploading signature file:', file.name);
    
    if (!file.type.match('image.*')) {
        alert('Please select an image file (PNG or JPG)');
        return;
    }
    
    const formData = new FormData();
    formData.append('file', file);
    
    fetch('/api/company/signature', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(result => {
        if (result.success) {
            const img = document.getElementById('signature-preview');
            img.src = STATIC_URL + result.filepath + '?t=' + Date.now();
            img.style.display = 'block';
            document.getElementById('no-signature').style.display = 'none';
            showMessage('Signature uploaded successfully!', 'success');
        } else {
            alert('Error uploading signature: ' + result.error);
        }
    })
    .catch(error => {
        console.error('Upload error:', error);
        alert('Error uploading signature');
    });
}

function showMessage(text, type) {
    const message = document.getElementById('message');
    message.textContent = text;
    message.className = `message ${type}`;
    message.style.display = 'block';
    
    setTimeout(() => {
        message.style.display = 'none';
    }, 3000);
}