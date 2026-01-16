// Bootstrap the AuthJS Client for ServiceNow
const authClient = new OktaAuth({
    url: "https://dev-16407622.okta.com",
    clientId: "0oadvrpixrp4jsDvz5d7",
    redirectUri: "https://your-redirect-uri.com",
    issuer: "https://dev-16407622.okta.com",
    scope: ['openid', 'profile', 'email']
});

// Initialize the Engagement Messenger
SN_CSM_EC.init({
    moduleID: "https://dev175885.service-now.com//#your-engagement-messenger-module-id",
    loadFeature: SN_CSM_EC.loadEMFeature()
});

// Function to display user information
function displayUserInfo() {
    const userInfoContainer = document.getElementById('userInfo');

    // Check if the user is authenticated
    if (authClient.tokenManager.get('idToken')) {
        const idToken = authClient.tokenManager.get('idToken');

        // Display user information
        userInfoContainer.innerHTML = `<p>User Info:</p>
                                       <p>Name: ${idToken.claims.name}</p>
                                       <p>Email: ${idToken.claims.email}</p>`;
    } else {
        userInfoContainer.innerHTML = '<p>User is not authenticated. Please login first.</p>';
    }
}

// Event listener for the "Get User Info" button click
document.getElementById('getUserInfoButton').addEventListener('click', displayUserInfo);

// Automatically display user information on page load
displayUserInfo();