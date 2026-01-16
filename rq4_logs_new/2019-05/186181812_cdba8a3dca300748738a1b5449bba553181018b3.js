const txtEmail = document.getElementById('txtEmail');
const txtPassword = document.getElementById('txtPassword');
const btnLogin = document.getElementById('btnLogin');
const btnSignUp = document.getElementById('btnSignUp');
const btnLogout = document.getElementById('btnLogout');
const loginMsg = document.getElementById('loginMsg');

UserLoggedIn = function(logged) {
	if(logged)
	{
		SetLoginMsg('User Logged');
		btnLogout.classList.remove('hide');
		btnLogin.classList.add('hide');
		btnSignUp.classList.add('hide');
	} else {
		SetLoginMsg('User Logged Out');
		btnLogout.classList.add('hide');
		btnLogin.classList.remove('hide');
		btnSignUp.classList.remove('hide');
	}
}

SetLoginMsg = function(msg) {
	loginMsg.innerHTML = msg;

	if (msg == '')
	{
		loginMsg.classList.add('hide');
	} else {
		loginMsg.classList.remove('hide');
	}
}

// Add login event
btnLogin.addEventListener('click', e => {
	// Get email and pass
	const email = txtEmail.value;
	const pass = txtPassword.value;
	const auth = firebase.auth();

	// Sign in
	const promise = auth.signInWithEmailAndPassword(email, pass);

	promise.catch(e => {
		SetLoginMsg(e.message);
	});
});

// Add signup event
btnSignUp.addEventListener('click', e => {
	// Get email and pass
	// TODO: check 4 real email
	const email = txtEmail.value;
	const pass = txtPassword.value;
	const auth = firebase.auth();

	// Create user
	const promise = auth.createUserWithEmailAndPassword(email, pass);

	promise.catch(e => {
		SetLoginMsg(e.message);
	});
});

// Logout
btnLogout.addEventListener('click', e => {
	SetLoginMsg('');
	firebase.auth().signOut();
});

// Add a realtime listener
firebase.auth().onAuthStateChanged(firebaseUser => {
	//console.log(firebaseUser);
	if(firebaseUser) {
		UserLoggedIn(true);
	} else {
		UserLoggedIn(false);
	}
});