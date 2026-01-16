using UnityEngine;

public enum Disks : sbyte
{
    First,
    Second,
    Third
}

public class Lock : MonoBehaviour
{
    [SerializeField] private LockCylinder[] disks;
    [SerializeField] private Transform handle;
    [SerializeField] private Transform pickupPosition;

    private Vector3 handleRot;
    private Vector3 rotation;
    private Vector3 code;


    private int cylinderA, cylinderB, cylinderC;
    private bool isCylinderA, isCylinderB, isCylinderC;

    public Transform PickupPosition
    {
        get => pickupPosition;
    }

    private int CylinderA
    {
        get => cylinderA;
        set
        {
            if(value > 9)
            {
                cylinderA = 0;
            }
            else
            {
                cylinderA = value;
            }
        }
    }

    private int CylinderB
    {
        get => cylinderB;
        set
        {
            if (value > 9)
            {
                cylinderB = 0;
            }
            else
            {
                cylinderB = value;
            }
        }
    }

    private int CylinderC
    {
        get => cylinderC;
        set
        {
            if (value > 9)
            {
                cylinderC = 0;
            }
            else
            {
                cylinderC = value;
            }
        }
    }

    private void Awake()
    {
        rotation = new Vector3(0, 36, 0);
        handleRot = new Vector3(50, 0, 0);
        code = new Vector3(6, 8, 1);
    }

    private void OpenLock()
    {
        transform.gameObject.AddComponent<Rigidbody>();
        handle.Rotate(handleRot);
        Destroy(gameObject, 15f);
    }

    [ContextMenu("Rotate Disk")]
    public void Rotate(Disks disk)
    {
        int diskId = (int)disk;
        disks[diskId].transform.Rotate(rotation);
        print("Rotating");
        switch (disk)
        {
            case Disks.First:
                if (++CylinderA == (int)code.x)
                {
                    isCylinderA = true;
                }
                else
                {
                    isCylinderA = false;
                }
                break;
            case Disks.Second:
                if (++CylinderB == (int)code.y)
                {
                    isCylinderB = true;
                }
                else
                {
                    isCylinderB = false;
                }
                break;
            case Disks.Third:
                if (++CylinderC == (int)code.z)
                {
                    isCylinderC = true;
                }
                else
                {
                    isCylinderC = false;
                }
                break;
        }

        if(isCylinderA == true && isCylinderB == true && isCylinderC == true)
        {
            // GenericEvents.s_instance.onSuccessPickCode.Invoke();
            // GenericEvents.s_instance.onEndPickCode.Invoke();
        }
    }
}