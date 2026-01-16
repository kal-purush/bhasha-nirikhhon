using UnityEngine;

public class HologramEffect : MonoBehaviour
{
    [Header("Hologram Settings")]
    [SerializeField] private Material hologramMaterial;
    [SerializeField] private float floatHeight = 1.5f;
    [SerializeField] private float bobSpeed = 1f;
    [SerializeField] private float bobAmount = 0.1f;
    [SerializeField] private float rotationSpeed = 15f;
    [SerializeField] private Color hologramColor = new Color(0, 0.8f, 0.8f, 0.7f);

    // References to the created hologram
    private GameObject hologramObject;
    private MeshRenderer hologramRenderer;
    private float initialHeight;

    public void CreateHologram()
    {
        if (hologramObject != null) return;
        // If a hologram already exists, destroy it first
        DestroyHologram();

        // Create a new GameObject for the hologram
        hologramObject = new GameObject(gameObject.name + "_Hologram");

        // CRUCIAL FIX: Make sure the hologram is not a child of anything
        // This ensures the animation can work without interference
        hologramObject.transform.parent = null;

        // Position it above the original object
        hologramObject.transform.position = transform.position + Vector3.up * floatHeight;

        // Copy rotation and scale but make it slightly smaller
        hologramObject.transform.rotation = transform.rotation;
        hologramObject.transform.localScale = transform.localScale * 0.95f;

        // Store the initial height for bobbing animation
        initialHeight = hologramObject.transform.position.y;

        // Copy the mesh from the original object
        CopyMesh();

        // Apply the hologram material
        ApplyHologramMaterial();

        // Add hologram animation script with more pronounced values
        HologramAnimation animation = hologramObject.AddComponent<HologramAnimation>();
        animation.Initialize(bobSpeed, bobAmount, rotationSpeed, initialHeight);

        // Debug message to confirm animation is set up
        Debug.Log($"Hologram animation initialized: BobSpeed={bobSpeed}, BobAmount={bobAmount}, RotationSpeed={rotationSpeed}");
    }

    private void CopyMesh()
    {
        // Get the mesh filter components
        MeshFilter originalMeshFilter = GetComponentInChildren<MeshFilter>();

        if (originalMeshFilter != null && originalMeshFilter.sharedMesh != null)
        {
            // Add mesh filter and mesh renderer to the hologram
            MeshFilter hologramMeshFilter = hologramObject.AddComponent<MeshFilter>();
            hologramRenderer = hologramObject.AddComponent<MeshRenderer>();

            // Copy the mesh
            hologramMeshFilter.mesh = originalMeshFilter.sharedMesh;
        }
        else
        {
            // Try to copy from skinned mesh renderer if standard mesh isn't found
            SkinnedMeshRenderer skinnedMeshRenderer = GetComponentInChildren<SkinnedMeshRenderer>();

            if (skinnedMeshRenderer != null)
            {
                // Create a new mesh from the skinned mesh
                Mesh bakedMesh = new Mesh();
                skinnedMeshRenderer.BakeMesh(bakedMesh);

                // Add the components and assign the mesh
                MeshFilter hologramMeshFilter = hologramObject.AddComponent<MeshFilter>();
                hologramRenderer = hologramObject.AddComponent<MeshRenderer>();
                hologramMeshFilter.mesh = bakedMesh;
            }
            else
            {
                Debug.LogWarning("No mesh found to create hologram from.");
                Destroy(hologramObject);
                hologramObject = null;
            }
        }
    }

    private void ApplyHologramMaterial()
    {
        if (hologramRenderer == null) return;

        // Create a new material instance
        if (hologramMaterial != null)
        {
            // Create an exact copy of the existing hologram material
            Material instancedMaterial = new Material(hologramMaterial);

            // Set the color properties (adjust property names based on your shader)
            if (instancedMaterial.HasProperty("_Color"))
            {
                instancedMaterial.SetColor("_Color", hologramColor);
            }

            if (instancedMaterial.HasProperty("_RimColor"))
            {
                instancedMaterial.SetColor("_RimColor", hologramColor);
            }

            if (instancedMaterial.HasProperty("_EmissionColor"))
            {
                instancedMaterial.SetColor("_EmissionColor", hologramColor);
            }

            // Assign the material to the renderer
            hologramRenderer.material = instancedMaterial;
        }
        else
        {
            Debug.LogError("No hologram material assigned! Please assign a material in the inspector.");
        }
    }

    // Update the hologram position when the original object moves
    private void Update()
    {
        if (hologramObject != null)
        {
            // Update the hologram's X and Z position to follow the original object
            // but leave Y to be controlled by the animation
            Vector3 newPosition = hologramObject.transform.position;
            newPosition.x = transform.position.x;
            newPosition.z = transform.position.z;
            hologramObject.transform.position = newPosition;
        }
    }

    public void DestroyHologram()
    {
        if (hologramObject != null)
        {
            Destroy(hologramObject);
            hologramObject = null;
        }
    }
}

// Modified Animation class for the hologram
public class HologramAnimation : MonoBehaviour
{
    private float bobSpeed;
    private float bobAmount;
    private float rotationSpeed;
    private float initialHeight;
    private bool isInitialized = false;

    public void Initialize(float bobSpeed, float bobAmount, float rotationSpeed, float initialHeight)
    {
        this.bobSpeed = bobSpeed;
        this.bobAmount = bobAmount;
        this.rotationSpeed = rotationSpeed;
        this.initialHeight = initialHeight;
        isInitialized = true;

        // Debug log to check if Initialize is being called
        Debug.Log($"HologramAnimation initialized with values: bobSpeed={bobSpeed}, bobAmount={bobAmount}, rotationSpeed={rotationSpeed}");
    }

    private void Update()
    {
        if (!isInitialized)
        {
            Debug.LogWarning("HologramAnimation not properly initialized!");
            return;
        }

        // Bob up and down
        float newY = initialHeight + Mathf.Sin(Time.time * bobSpeed) * bobAmount;
        Vector3 currentPos = transform.position;
        transform.position = new Vector3(currentPos.x, newY, currentPos.z);

        // Rotate slowly
        transform.Rotate(Vector3.up, rotationSpeed * Time.deltaTime);
    }
}