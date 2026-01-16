document.querySelectorAll('.number__format').forEach(node => {
    node.textContent = new Intl.NumberFormat('en-US', { 
        style: 'currency', 
        currency: 'CAD' 
        }).format(node.textContent)
    })