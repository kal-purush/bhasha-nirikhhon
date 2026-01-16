import React, { useEffect, useState } from 'react';

function FetchAccountInfo() {
  const [accountInfo, setAccountInfo] = useState(null);

  useEffect(() => {
    const options = {
      method: 'POST',
      headers: {'Content-type': 'application/json'},
      body: '{"access_token":"79bf8b4980164e45d75a5799937acc11e80a68ba"}'
    };

    fetch('https://keep2share.cc/api/v2/accountInfo', options)
      .then(response => response.json())
      .then(response => setAccountInfo(response))
      .catch(err => console.error(err));
  }, []);

  return (
    <div>
      {accountInfo && (
        <header>
          <h1>Welcome, {accountInfo.username}!</h1>
          <p>Your email is {accountInfo.email}.</p>
        </header>
      )}
    </div>
  );
}

export default FetchAccountInfo;