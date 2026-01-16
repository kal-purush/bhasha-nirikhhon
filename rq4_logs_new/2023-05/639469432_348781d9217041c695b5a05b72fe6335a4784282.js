import React, { useEffect } from 'react';

function GoogleSignInButton() {
  useEffect(() => {
    const loadGoogleAPI = () => {
      const script = document.createElement('script');
      script.src = 'https://accounts.google.com/gsi/client';
      script.onload = initializeGoogleSignIn;
      document.body.appendChild(script);
    };

    const initializeGoogleSignIn = () => {
      window.google.accounts.id.initialize({
        client_id: 'YOUR_GOOGLE_CLIENT_ID',
        callback: handleGoogleSignIn,
      });
    };

    const handleGoogleSignIn = (response) => {
      const { credential } = response;
      const idToken = credential.id;

      // Use the ID token for further authentication or API requests
      console.log(idToken);
    };

    loadGoogleAPI();

    return () => {
      const script = document.querySelector('script[src="https://accounts.google.com/gsi/client"]');
      if (script) {
        script.remove();
      }
    };
  }, []);

  return (
    <button className="decoration-blue-300 bg-green-500 hover:bg-green-700 text-white  py-2 px-4 rounded flex items-center justify-center">
      Sign in with Google
    </button>
  );
}

export default GoogleSignInButton;