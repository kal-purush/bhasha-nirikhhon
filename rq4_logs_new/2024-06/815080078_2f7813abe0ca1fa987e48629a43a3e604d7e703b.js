
    // Function to display the delete modal and set the maintenance ID
    function displaydeleteModal(maintID) {
        var modalEdit = document.getElementById("DeletemyModal");
        // Store the maintenance ID in a hidden field or a JavaScript variable
        document.getElementById("deleteMaintID").value = maintID;
        
        document.getElementById('deleteMaintenanceForm').action = "./maintenance.php?maintID=" + maintID;
    }


    