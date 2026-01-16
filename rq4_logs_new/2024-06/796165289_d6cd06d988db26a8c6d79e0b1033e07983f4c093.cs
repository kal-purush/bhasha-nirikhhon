using UnityEngine;
using Unity.Services.Core;
using Unity.Services.Core.Environments;
using UnityEngine.Purchasing;
using System.Threading.Tasks;
using Firebase.Database;

public class IAPManager : MonoBehaviour, IStoreListener
{
    private static IStoreController storeController;
    private static IExtensionProvider storeExtensionProvider;
    private DatabaseReference reference;

    public static string subscriptionProductId = "com.yourgame.subscription";

    public GameObject physicsFreeGameObject;
    public GameObject chemistryFreeGameObject;
    public GameObject biologyFreeGameObject;

    public GameObject physicsPremiumGameObject;
    public GameObject chemistryPremiumGameObject;
    public GameObject biologyPremiumGameObject;

    private bool isPaidUser;

    async void Start()
    {
        var options = new InitializationOptions()
                     .SetEnvironmentName("production");
        await UnityServices.InitializeAsync(options);
        Debug.Log("Unity Services Initialized Successfully!");
        
        reference = FirebaseDatabase.DefaultInstance.RootReference;

        InitializeUser();
        await CheckPaymentStatus();

        if (storeController == null)
        {
            InitializePurchasing();
        }
    }

    private void InitializeUser()
    {
        // Generate a unique UserId for the device if it doesn't already exist
        if (!PlayerPrefs.HasKey("UserId"))
        {
            string userId = SystemInfo.deviceUniqueIdentifier;
            PlayerPrefs.SetString("UserId", userId);
            PlayerPrefs.Save();

            // Create a new user entry in Firebase
            CreateUser(userId);
        }
    }

    private void CreateUser(string userId)
    {
        User newUser = new User(0);  // Phone number is not relevant here anymore
        string json = JsonUtility.ToJson(newUser);
        reference.Child("users").Child(userId).SetRawJsonValueAsync(json).ContinueWith(task => {
            if (task.IsFaulted)
            {
                Debug.LogError("Error creating user: " + task.Exception);
            }
            else if (task.IsCompleted)
            {
                Debug.Log("User created successfully");
            }
        });
    }

    public void InitializePurchasing()
    {
        var builder = ConfigurationBuilder.Instance(StandardPurchasingModule.Instance());
        builder.AddProduct(subscriptionProductId, ProductType.Subscription);
        UnityPurchasing.Initialize(this, builder);
    }

    private bool IsInitialized()
    {
        return UnityServices.State == ServicesInitializationState.Initialized;
    }

    public void OnInitialized(IStoreController controller, IExtensionProvider extensions)
    {
        storeController = controller;
        storeExtensionProvider = extensions;
    }

    public void OnInitializeFailed(InitializationFailureReason error)
    {
        Debug.Log("Unity IAP Initialization Failed: " + error);
    }

    public void BuySubscription()
    {
        if (storeController != null)
        {
            storeController.InitiatePurchase(subscriptionProductId);
        }
        else
        {
            Debug.Log("StoreController is not initialized");
        }
    }

    public void OnPurchaseFailed(Product product, PurchaseFailureReason failureReason)
    {
        Debug.Log("Purchase failed: " + failureReason);
    }

    public PurchaseProcessingResult ProcessPurchase(PurchaseEventArgs args)
    {
        if (args.purchasedProduct.definition.id == subscriptionProductId)
        {
            Debug.Log("Subscription purchased");
            PlayerPrefs.SetInt("SubscriptionActive", 1);
            SetPaidUserStatus(true);
        }
        return PurchaseProcessingResult.Complete;
    }

    public async Task CheckPaymentStatus()
    {
        string userId = PlayerPrefs.GetString("UserId");
        if (string.IsNullOrEmpty(userId))
        {
            Debug.Log("UserId is not set in PlayerPrefs");
            isPaidUser = false;
            return;
        }

        DataSnapshot snapshot = await reference.Child("users").Child(userId).GetValueAsync();

        if (snapshot.Exists)
        {
            User user = JsonUtility.FromJson<User>(snapshot.GetRawJsonValue());
            isPaidUser = user.IsPaidUser;
        }
        else
        {
            isPaidUser = false;
        }

        // Ensure local state matches the remote state
        PlayerPrefs.SetInt("SubscriptionActive", isPaidUser ? 1 : 0);
        PlayerPrefs.Save();
    }

    public void OnSubjectButtonClicked(string subject)
    {
        isPaidUser = PlayerPrefs.GetInt("SubscriptionActive", 0) == 1;

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

    private async void SetPaidUserStatus(bool status)
    {
        string userId = PlayerPrefs.GetString("UserId");
        await reference.Child("users").Child(userId).Child("IsPaidUser").SetValueAsync(status);
        isPaidUser = status;
    }

    public void OnInitializeFailed(InitializationFailureReason error, string message)
    {
        Debug.LogError($"Unity IAP Initialization Failed: {error}, {message}");
    }
}