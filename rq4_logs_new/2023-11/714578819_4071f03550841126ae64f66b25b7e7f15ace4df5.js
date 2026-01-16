// PasswordReset.js
import { getAuth, sendPasswordResetEmail } from 'https://www.gstatic.com/firebasejs/10.6.0/firebase-auth.js';
import { getFirestore, collection, query, where, getDocs } from 'https://www.gstatic.com/firebasejs/10.6.0/firebase-firestore.js';

export const resetPassword = async function(cin, email, error, errorL, loading, passwordResetSent, router) {
  try {
    const auth = getAuth();
    const db = getFirestore();

    if (!email || !cin) {
      error.value = 'Veuillez remplir tous les champs.';
      return;
    }

    // Set loading to true to display the loading spinner
    loading.value = true;

    const usersRef = collection(db, 'users');
    const usersQuery = query(usersRef, where('cin', '==', cin));
    const usersSnapshot = await getDocs(usersQuery);

    if (usersSnapshot.empty) {
      error.value = 'La CIN fournie n’a pas été trouvée.';
      return;
    }

    const userDoc = usersSnapshot.docs[0].data();
    if (userDoc.email !== email) {
      error.value = 'L\'adresse email ne correspond pas à la CIN fournie.';
      return;
    }

    // Send password reset email
    await sendPasswordResetEmail(auth, email);
    errorL.value = false;
    // Update state variable to indicate successful password reset email
    passwordResetSent.value = true;

    // Delay for 3 seconds before navigating to the login page
    setTimeout(() => {
      router.push('/login');
    }, 3000);
  } catch (error) {
    console.error('Password reset failed:', error.message);
    error.value = 'Échec de la réinitialisation du mot de passe ';
  } finally {
    // Set loading to false after the operation is complete
    loading.value = false;
  }
};