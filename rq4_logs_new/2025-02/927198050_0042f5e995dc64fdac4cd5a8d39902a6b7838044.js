async function fetchUserData() {
    const apiUrl = "https://b42web03webwizards-default-rtdb.asia-southeast1.firebasedatabase.app/users.json";
    const loggedInUserId = sessionStorage.getItem("userId");

    if (!loggedInUserId) {
        console.error("No user logged in!");
        return;
    }

    try {
        const response = await fetch(apiUrl);
        const data = await response.json();

        if (data && data[loggedInUserId]) {
            const userData = data[loggedInUserId];

            let fullName = userData.name || "John Doe";
            let nameParts = fullName.split(" ");
            let firstName = nameParts[0] || "";
            let lastName = nameParts.slice(1).join(" ") || "";

            const inputs = {
                firstName: document.querySelector(".text-input[name='firstName']"),
                lastName: document.querySelector(".text-input[name='lastName']"),
                email: document.querySelector(".text-input[name='email']"),
                mobile: document.querySelector(".text-input[name='number']"),
                gender: document.querySelector(".text-input[name='gender']"),
                address: document.querySelector(".text-input[name='address']"),
                zipCode: document.querySelector(".text-input[name='zipCode']")
            };

            if (!inputs.firstName || !inputs.lastName) {
                console.error("Input fields not found in the DOM!");
                return;
            }

            inputs.firstName.value = userData.firstName || firstName;
            inputs.lastName.value = userData.lastName || lastName;
            inputs.email.value = userData.email || "";
            inputs.mobile.value = userData.mobile || "";
            inputs.gender.value = userData.gender || "";
            inputs.address.value = userData.address || "";
            inputs.zipCode.value = userData.zipCode || "";

            document.querySelector(".user-name").textContent = fullName;
            document.querySelector(".user-role").textContent = userData.role || "Administrator";

            Object.values(inputs).forEach(input => input.setAttribute("readonly", true));
        } else {
            console.log("User data not found!");
        }
    } catch (error) {
        console.error("Error fetching data:", error);
    }
}

document.addEventListener("DOMContentLoaded", fetchUserData);

document.querySelector(".btn.modify").addEventListener("click", function () {
    document.querySelectorAll(".text-input").forEach(input => input.removeAttribute("readonly"));
});

document.querySelector(".btn.update").addEventListener("click", async function () {
    const loggedInUserId = sessionStorage.getItem("userId");
    if (!loggedInUserId) return console.log("No user logged in!");

    const inputs = {
        firstName: document.querySelector(".text-input[name='firstName']"),
        lastName: document.querySelector(".text-input[name='lastName']"),
        email: document.querySelector(".text-input[name='email']"),
        mobile: document.querySelector(".text-input[name='number']"),
        gender: document.querySelector(".text-input[name='gender']"),
        address: document.querySelector(".text-input[name='address']"),
        zipCode: document.querySelector(".text-input[name='zipCode']")
    };

    const updatedData = {
        firstName: inputs.firstName.value,
        lastName: inputs.lastName.value,
        email: inputs.email.value,
        mobile: inputs.mobile.value,
        gender: inputs.gender.value,
        address: inputs.address.value,
        zipCode: inputs.zipCode.value
    };

    try {
        await fetch(`https://b42web03webwizards-default-rtdb.asia-southeast1.firebasedatabase.app/users/${loggedInUserId}.json`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(updatedData)
        });

        alert("Updated Successfully");

        Object.values(inputs).forEach(input => input.setAttribute("readonly", true));
        document.querySelector(".user-name").textContent = `${updatedData.firstName} ${updatedData.lastName}`;
    } catch (error) {
        console.error("Error updating data:", error);
    }
});

document.querySelector(".btn.discard").addEventListener("click", function () {
    fetchUserData();
    document.querySelectorAll(".text-input").forEach(input => input.setAttribute("readonly", true));
});