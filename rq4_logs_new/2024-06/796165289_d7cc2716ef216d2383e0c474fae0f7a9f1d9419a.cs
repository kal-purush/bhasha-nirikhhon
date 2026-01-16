using UnityEngine;
using UnityEngine.UI;
using TMPro;
using UnityEngine.SceneManagement;

public class CollisionCalculator : MonoBehaviour
{
    public Slider leftCarMassSlider;
    public Slider leftCarVelocitySlider;
    public Slider rightCarMassSlider;
    public Slider rightCarVelocitySlider;
    public TextMeshProUGUI leftCarMassText;
    public TextMeshProUGUI leftCarVelocityText;
    public TextMeshProUGUI leftCarMomentumText;
    public TextMeshProUGUI rightCarMassText;
    public TextMeshProUGUI rightCarVelocityText;
    public TextMeshProUGUI rightCarMomentumText;
    public GameObject leftCar;
    public GameObject rightCar;
    private Rigidbody2D leftCarRb;
    private Rigidbody2D rightCarRb;
    private bool isCollisionStarted = false;
    public AudioSource collisionSound;
    public GameObject buttonPrefab;
    public Slider collisionTypeSlider;
    public GameObject newLeftCarPrefab;
    public GameObject newRightCarPrefab;

    void Start()
    {
        leftCarRb = leftCar.GetComponent<Rigidbody2D>();
        rightCarRb = rightCar.GetComponent<Rigidbody2D>();
        leftCarMassSlider.wholeNumbers = true;
        leftCarVelocitySlider.wholeNumbers = true;
        rightCarMassSlider.wholeNumbers = true;
        rightCarVelocitySlider.wholeNumbers = true;

        UpdateValues();
        leftCarMassSlider.onValueChanged.AddListener(delegate { UpdateValues(); });
        leftCarVelocitySlider.onValueChanged.AddListener(delegate { UpdateValues(); });
        rightCarMassSlider.onValueChanged.AddListener(delegate { UpdateValues(); });
        rightCarVelocitySlider.onValueChanged.AddListener(delegate { UpdateValues(); });

        // Find the persistent AudioSource (collisionSound)
        GameObject audioManager = GameObject.FindGameObjectWithTag("AudioManager");
        if (audioManager != null)
        {
            collisionSound = audioManager.GetComponent<AudioSource>();
        }
        else
        {
            Debug.LogError("AudioManager not found. Please ensure there is a GameObject tagged 'AudioManager' with an AudioSource component.");
        }
    }
    void Awake()
    {
        Screen.orientation = ScreenOrientation.LandscapeLeft;
    }

    void UpdateValues()
    {
        int m1 = (int)leftCarMassSlider.value;
        int u1 = (int)leftCarVelocitySlider.value;
        int m2 = (int)rightCarMassSlider.value;
        int u2 = (int)rightCarVelocitySlider.value;

        leftCarMassText.text = m1.ToString("F2") + " kg";
        leftCarVelocityText.text = u1.ToString("F2") + " m/s";
        leftCarMomentumText.text = (m1 * u1).ToString("F2") + " kg·m/s";

        rightCarMassText.text = m2.ToString("F2") + " kg";
        rightCarVelocityText.text = u2.ToString("F2") + " m/s";
        rightCarMomentumText.text = (m2 * u2).ToString("F2") + " kg·m/s";
    }

    public void StartCollision()
    {
        float u1 = leftCarVelocitySlider.value;
        float u2 = rightCarVelocitySlider.value;

        float scalingFactor = 0.1f; // Adjust this value to control the speed

        leftCarRb.velocity = new Vector2(u1 * scalingFactor, 0);
        rightCarRb.velocity = new Vector2(u2 * scalingFactor, 0);
        isCollisionStarted = true;

        Debug.Log("StartCollision called: " + "Left Car Initial Velocity: " + u1 + ", Right Car Initial Velocity: " + u2);
    }

    void OnCollisionEnter2D(Collision2D collision)
    {
        Debug.Log("OnCollisionEnter2D called with: " + collision.gameObject.name);
        if (isCollisionStarted && (collision.gameObject == rightCar || collision.gameObject == leftCar))
        {
            Debug.Log("Collision detected");
            float v1, v2;

            //play the collision sound
            collisionSound.Play();

            if (collisionTypeSlider.value == 1)
            {
                CalculateInElasticCollision(out v1, out v2);

                // Play the collision sound for inelastic collision
                if (collisionSound != null)
                {
                    collisionSound.Play();
                }
                leftCar.SetActive(false);
                rightCar.SetActive(false);

                // Instantiate new cars
                GameObject newLeftCar = Instantiate(newLeftCarPrefab, leftCar.transform.position, Quaternion.identity);
                GameObject newRightCar = Instantiate(newRightCarPrefab, rightCar.transform.position, Quaternion.identity);

                // Destroy the old GameObjects

                // Assign the new GameObjects to the leftCar and rightCar fields
                leftCar = newLeftCar;
                rightCar = newRightCar;

                // Re-assign Rigidbody2D components
                leftCarRb = leftCar.GetComponent<Rigidbody2D>();
                rightCarRb = rightCar.GetComponent<Rigidbody2D>();

                // Apply calculated velocities to the new cars
                leftCarRb.velocity = new Vector2(v1 * 0.1f, 0);
                rightCarRb.velocity = new Vector2(v2 * 0.1f, 0);
            }
            else
            {
                CalculateElasticCollision(out v1, out v2);
            }
            isCollisionStarted = false; // Reset to prevent multiple calculations
            InstantiateButton();
        }
    }

    private void InstantiateButton()
{
    Vector3 worldPosition = new Vector3(0.3f, -3.2f, 0f);
    GameObject button = Instantiate(buttonPrefab);

    // Find the Canvas in your scene
    Canvas canvas = FindObjectOfType<Canvas>();

    // If a Canvas exists, set it as the parent of the button
    if (canvas != null)
    {
        button.transform.SetParent(canvas.transform, false);

        // Convert world position to Canvas position
        Vector2 canvasPosition;
        RectTransformUtility.ScreenPointToLocalPointInRectangle(
            canvas.GetComponent<RectTransform>(), 
            Camera.main.WorldToScreenPoint(worldPosition), 
            null, 
            out canvasPosition
        );

        // Set the position of the button
        button.GetComponent<RectTransform>().anchoredPosition = canvasPosition;

        // Get the Button component from the instantiated button
        Button buttonComponent = button.GetComponent<Button>();

        // Add an OnClick event listener to the button
        buttonComponent.onClick.AddListener(ReloadScene);
    }
    else
    {
        Debug.LogError("Canvas not found. Please ensure there is a Canvas in the scene.");
    }
}
    void ReloadScene()
    {
        int currentSceneIndex = SceneManager.GetActiveScene().buildIndex;
        SceneManager.LoadScene(currentSceneIndex);
        Debug.Log("Button Clicked!");
    }


    void CalculateElasticCollision(out float v1, out float v2)
    {
        float m1 = leftCarMassSlider.value;
        float u1 = leftCarVelocitySlider.value;
        float m2 = rightCarMassSlider.value;
        float u2 = rightCarVelocitySlider.value;
        Debug.Log("StartCollision called: " + "Left Car Initial Velocity: " + u1 + ", Right Car Initial Velocity: " + u2);

        // Calculate final velocities using derived formulas
        v1 = ((m1 - m2) * u1 + 2 * m2 * u2) / (m1 + m2);
        v2 = ((m2 - m1) * u2 + 2 * m1 * u1) / (m1 + m2);

        float scalingFactor = 0.1f; // Adjust this value to control the speed

        // Apply the new velocities with scaling factor
        leftCarRb.velocity = new Vector2(v1 * scalingFactor, 0);
        rightCarRb.velocity = new Vector2(v2 * scalingFactor, 0);

        // Log values for debugging
        Debug.Log($"Left Car: Initial Velocity = {u1}, Final Velocity = {v1}");
        Debug.Log($"Right Car: Initial Velocity = {u2}, Final Velocity = {v2}");

        // Update displayed values
        UpdateValuesAfterCollision(v1, v2);
    }
    void CalculateInElasticCollision(out float v1, out float v2)
    {
        float m1 = leftCarMassSlider.value;
        float u1 = leftCarVelocitySlider.value;
        float m2 = rightCarMassSlider.value;
        float u2 = rightCarVelocitySlider.value;
        Debug.Log("StartCollision called: " + "Left Car Initial Velocity: " + u1 + ", Right Car Initial Velocity: " + u2);

        // Calculate final velocities using derived formulas
        float v = (m1 * u1 + m2 * u2) / (m1 + m2);
        v1 = v;
        v2 = v;
        float scalingFactor = 0.1f; // Adjust this value to control the speed

        // Apply the new velocities with scaling factor
        leftCarRb.velocity = new Vector2(v1 * scalingFactor, 0);
        rightCarRb.velocity = new Vector2(v2 * scalingFactor, 0);

        // Log values for debugging
        Debug.Log($"Left Car: Initial Velocity = {u1}, Final Velocity = {v1}");
        Debug.Log($"Right Car: Initial Velocity = {u2}, Final Velocity = {v2}");

        // Update displayed values
        UpdateValuesAfterCollision(v1, v2);
    }


    void UpdateValuesAfterCollision(float v1, float v2)
    {
        float m1 = leftCarMassSlider.value;
        float m2 = rightCarMassSlider.value;

        leftCarVelocityText.text = v1.ToString("F2") + " m/s";
        leftCarMomentumText.text = (m1 * v1).ToString("F2") + " kg·m/s";

        rightCarVelocityText.text = v2.ToString("F2") + " m/s";
        rightCarMomentumText.text = (m2 * v2).ToString("F2") + " kg·m/s";

        Debug.Log($"Updated Values: Left Car - Velocity: {v1}, Momentum: {m1 * v1}");
        Debug.Log($"Updated Values: Right Car - Velocity: {v2}, Momentum: {m2 * v2}");
    }
}