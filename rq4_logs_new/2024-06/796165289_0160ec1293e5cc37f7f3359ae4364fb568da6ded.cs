using UnityEngine;
using Firebase.Database;
using System.Threading.Tasks;
using System.Collections;
using TMPro;
using UnityEngine.UI;
using System;

public class ExampleUsage : MonoBehaviour
{
    private PhonePePayment phonePePayment;
    private DatabaseReference reference;

    public GameObject physicsFreeGameObject;
    public GameObject chemistryFreeGameObject;
    public GameObject biologyFreeGameObject;

    public GameObject physicsPremiumGameObject;
    public GameObject chemistryPremiumGameObject;
    public GameObject biologyPremiumGameObject;
    public GameObject gameObject1; // Pay page
    public GameObject gameObject2; // Premium confirmation page
    public TextMeshProUGUI UserIDText;
    public Button copyButton;
    private string UserID;

    private bool isPaidUser;

    async void Start()
    {
        phonePePayment = GetComponent<PhonePePayment>();
        reference = FirebaseDatabase.DefaultInstance.RootReference;

        UserID = PlayerPrefs.GetString("UserID");
        if (string.IsNullOrEmpty(UserID))
        {
            Debug.LogError("UserID is not set in PlayerPrefs");
            // Handle the case where UserID is not set
            return;
        }
        UserIDText.text = $"{UserID}";
        copyButton.onClick.AddListener(CopyUserIDToClipboard);
        await CheckPaymentStatus(UserID);
    }

    public async Task CheckPaymentStatus(string UserID)
    {
        if (string.IsNullOrEmpty(UserID))
        {
            Debug.LogError("UserID is null or empty.");
            isPaidUser = false;
            return;
        }

        try
        {
            DataSnapshot snapshot = await reference.Child("users").Child(UserID).GetValueAsync();

            if (snapshot.Exists)
            {
                MyUser user = JsonUtility.FromJson<MyUser>(snapshot.GetRawJsonValue());
                Debug.Log($"User retrieved from database: {user.IsPaidUser}");
                isPaidUser = user.IsPaidUser;
            }
            else
            {
                Debug.LogError("User snapshot does not exist in the database.");
                isPaidUser = false;
            }
        }
        catch (Exception ex)
        {
            Debug.LogError($"Error retrieving user data: {ex.Message}");
            isPaidUser = false;
        }

        Debug.Log($"isPaidUser set to: {isPaidUser}");
    }

    public async void OnPayButtonClicked()
    {
        string UserID = PlayerPrefs.GetString("UserID");
        if (string.IsNullOrEmpty(UserID))
        {
            Debug.LogError("UserID is not set in PlayerPrefs");
            return;
        }

        try
        {
            DataSnapshot snapshot = await reference.Child("users").Child(UserID).GetValueAsync();

            if (snapshot.Exists)
            {
                MyUser user = JsonUtility.FromJson<MyUser>(snapshot.GetRawJsonValue());
                long phoneNumber = user.PhoneNumber; // Retrieve the phone number from the user data
                float amount = 199.0f; // Example amount
                await InitiatePaymentAndUnlock(amount, phoneNumber, UserID);
            }
        }
        catch (Exception ex)
        {
            Debug.LogError($"Error retrieving user data for payment: {ex.Message}");
        }
    }

    private async Task InitiatePaymentAndUnlock(float amount, long phoneNumber, string UserID)
    {
        bool paymentSuccess = await InitiatePaymentAsync(amount, phoneNumber);

        if (paymentSuccess)
        {
            try
            {
                // Update the user's payment status in the database
                await reference.Child("users").Child(UserID).Child("IsPaidUser").SetValueAsync(true);

                // Update the local payment status
                isPaidUser = true;

                Debug.Log("Premium content unlocked.");
            }
            catch (Exception ex)
            {
                Debug.LogError($"Error updating payment status: {ex.Message}");
            }
        }
        else
        {
            Debug.LogError("Payment failed");
        }
    }

    private Task<bool> InitiatePaymentAsync(float amount, long phoneNumber)
    {
        var tcs = new TaskCompletionSource<bool>();

        StartCoroutine(InitiatePaymentCoroutine(amount, phoneNumber, tcs));

        return tcs.Task;
    }

    private IEnumerator InitiatePaymentCoroutine(float amount, long phoneNumber, TaskCompletionSource<bool> tcs)
    {
        yield return phonePePayment.InitiatePayment(amount, phoneNumber.ToString());

        tcs.SetResult(phonePePayment.PaymentSuccess);
    }

    public void OnSubjectButtonClicked(string subject)
    {
        if (isPaidUser)
        {
            switch (subject)
            {
                case "physics":
                    physicsPremiumGameObject.SetActive(true);
                    physicsFreeGameObject.SetActive(false);
                    break;
                case "chemistry":
                    chemistryPremiumGameObject.SetActive(true);
                    chemistryFreeGameObject.SetActive(false);
                    break;
                case "biology":
                    biologyPremiumGameObject.SetActive(true);
                    biologyFreeGameObject.SetActive(false);
                    break;
            }
        }
        else
        {
            switch (subject)
            {
                case "physics":
                    physicsFreeGameObject.SetActive(true);
                    physicsPremiumGameObject.SetActive(false);
                    break;
                case "chemistry":
                    chemistryFreeGameObject.SetActive(true);
                    chemistryPremiumGameObject.SetActive(false);
                    break;
                case "biology":
                    biologyFreeGameObject.SetActive(true);
                    biologyPremiumGameObject.SetActive(false);
                    break;
            }
        }
    }

    public void OnPayPageButtonClicked()
    {
        if (isPaidUser)
        {
            gameObject1.SetActive(false);
            gameObject2.SetActive(true);
        }
        else
        {
            gameObject1.SetActive(true);
            gameObject2.SetActive(false);
        }
    }

    // Method to copy the user ID to the clipboard
    public void CopyUserIDToClipboard()
    {
        GUIUtility.systemCopyBuffer = UserID;
        Debug.Log($"User ID {UserID} copied to clipboard");
    }
}

[System.Serializable]
public class MyUser
{
    public string UserID;
    public long PhoneNumber;
    public bool IsPaidUser;
}