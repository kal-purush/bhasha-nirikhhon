using System.Reflection;
using System.Linq;
using Harmony;
using UnityEngine;
using UnityEngine.Audio;

[ModEntryPoint]
public class Patch
{
    public static void Main()
    {
        var harmony = HarmonyInstance.Create("com.github.johnnyonflame.rangerhup");
        harmony.PatchAll(Assembly.GetExecutingAssembly());
    }

    const float pm_stopspeed = 100.0f;
    const float pm_duckScale = 0.25f;
    const float pm_swimScale = 0.50f;
    const float pm_swimGravityScale = 0.075f;
    const float pm_wadeScale = 0.70f;
    const float pm_accelerate = 10.0f;
    const float pm_airaccelerate = 1.0f;
    const float pm_wateraccelerate = 4.0f;
    const float pm_friction = 6.0f;
    const float pm_waterfriction = 1.0f;
    const float pm_jumpvelocity = 270.0f;
    const float pm_gravity = 800.0f;
    const float pm_maxvelocity = 320.0f;

    const bool sv_autohop = true;
    static MyControllerScript disbecontroller = null;
    static CharacterController disbecharacontroller = null;
    static ClimbingPowerupScript disbeclimbing = null;
    static LayerMask ladderlayers;
    static int waterlayer = 0;
    static Vector3 velocity = Vector3.zero;
    static bool onLadders = false;
    static bool inwater_override = false;
    static float waterlevel = 0f;
    static Vector3 prev = new Vector3(Mathf.Infinity, Mathf.Infinity, Mathf.Infinity);

    public static void GetInput(MyControllerScript __instance, bool isInWater)
    {
        /* Player Directional Input */
        Transform transform = (__instance.inwater && __instance.SixDegreesOfFreedomSwimming)
                               ? __instance.dascam.transform
                               : __instance.transform;
        __instance.InputDir = transform.right   /* D/A */
                               * ((__instance.inputmanager.GetKeyInput("right",    0) ?  1 : 0)
                               +  (__instance.inputmanager.GetKeyInput("left",     0) ? -1 : 0))
                            + transform.forward /* W/S */
                               * ((__instance.inputmanager.GetKeyInput("forward",  0) ?  1 : 0)
                               +  (__instance.inputmanager.GetKeyInput("backward", 0) ? -1 : 0));
        if (isInWater)
        {
            __instance.InputDir += transform.up /* W/S */
                                   * ((__instance.inputmanager.GetKeyInput("jump",   0) ?  1 : 0)
                                   +  (__instance.inputmanager.GetKeyInput("crouch", 0) ? -1 : 0));
        }
        __instance.InputDir = __instance.InputDir.normalized;

        /* Walk/Run states */
        __instance.MaxRunSpeed = pm_maxvelocity;
        __instance.InputAccel = pm_accelerate;
        if (!(__instance.inputmanager.GetKeyInput("walk", 0) ^ __instance.runtoggle))
        {
            __instance.InputAccel *= 0.75f;
            __instance.MaxRunSpeed *= 0.75f;
        }

        /* Jumping State */
        __instance.dojump = __instance.inputmanager.GetKeyInput("jump", (sv_autohop) ? 0 : 1);

        /* Crouch/Stand States */
        if (__instance.inputmanager.GetKeyInput("crouch", 1))
        {
            __instance.CrouchState = (__instance.crouchtoggle) ? !__instance.CrouchState : true;
        }
        else if (!__instance.crouchtoggle && !__instance.inputmanager.GetKeyInput("crouch", 0))
        {
            __instance.CrouchState = false;
        }

        /*if (__instance.inwater)
            __instance.CrouchState = true;*/
    }

    private static Vector3 PM_Accelerate(Vector3 wishdir, Vector3 vel, float accel, float wishspeed)
    {
        float addspeed, accelspeed, currentspeed;
        currentspeed = Vector3.Dot(vel, wishdir);
        addspeed = wishspeed - currentspeed;
        if (addspeed <= 0)
            return vel;

        accelspeed = accel * Time.deltaTime * wishspeed;
        return vel + accelspeed * wishdir;
    }

    private static Vector3 PM_Friction(Vector3 vel, float friction, float stopspeed)
    {
        float control, newspeed, speed = vel.magnitude;
        if (speed != 0)
        {
            control = (speed < stopspeed && !(disbecontroller.inwater || inwater_override)) ? stopspeed : speed;
            newspeed = Mathf.Max(0, speed - Time.deltaTime * control * friction) / speed;
            vel *= newspeed;
        }

        return vel;
    }

    private static void DoSpidermanMovement(MyControllerScript __instance)
    {
        velocity = __instance.wallmovement * 50.0f * 25.0f;
    }

    private static void DoWaterMovement(MyControllerScript __instance)
    {
        //If not pressing anything, then sink.
        if ((__instance.InputDir.magnitude == 0) || (waterlevel < 2))
            velocity.y -= pm_gravity * Time.deltaTime * pm_swimGravityScale;

        velocity = PM_Friction(velocity, pm_waterfriction * waterlevel, pm_stopspeed);
        velocity = PM_Accelerate(__instance.InputDir, velocity, pm_wateraccelerate, __instance.MaxRunSpeed * pm_swimScale);
    }

    private static void DoGroundMovement(MyControllerScript __instance)
    {
        if (__instance.CrouchState)
        {
            __instance.MaxRunSpeed *= pm_duckScale;
        }

        if (__instance.dojump)
        {
            velocity.y = pm_jumpvelocity;
        }
        else
        {
            if (velocity.y < 0 && __instance.CheckGrounded())
                velocity.y = 0f;

            velocity = PM_Friction(velocity, pm_friction, pm_stopspeed);
        }

        velocity = PM_Accelerate(__instance.InputDir, velocity, __instance.InputAccel, __instance.MaxRunSpeed);
    }

    private static void DoAirMovement(MyControllerScript __instance)
    {
        if (onLadders)
        {
            velocity = PM_Friction(velocity, pm_friction * 1.5f, pm_stopspeed);
            velocity.y = 0;
            velocity = PM_Accelerate(__instance.InputDir, velocity, __instance.InputAccel * 0.25f, __instance.MaxRunSpeed);

            /* Raycast into our current running direction to see if we're climbing */
            Vector3 ladderSpherePoint = __instance.transform.position;

            RaycastHit hit;
            if (Physics.Raycast(new Ray(ladderSpherePoint, __instance.InputDir), out hit, 1, ladderlayers))
            {
                /*
                 * ::TODO::
                 * investigate if this can be improved in order to make the character climb up and down.
                 * maybe by using the dot product to prevent backwards movement and using that?
                 */
                velocity.y = -Vector3.Dot(hit.normal, __instance.InputDir) * __instance.MaxRunSpeed;
            }

            if (__instance.dojump)
            {
                velocity = __instance.dascam.transform.rotation * Vector3.forward * pm_jumpvelocity;
                velocity.y += pm_jumpvelocity; /* Fix for a very specific spot in the game */
            }
        }
        else
        {
            velocity = PM_Accelerate(__instance.InputDir, velocity, pm_airaccelerate, __instance.MaxRunSpeed);
        }
    }

    [HarmonyPatch(typeof(MyControllerScript), "Update")]
    class MyControllerScript_Update_Patch
    {
        static bool Prefix(MyControllerScript __instance)
        {
            /* Cache a few useful things */
            if (!disbecontroller)
            {
                disbecontroller = __instance;
                prev = __instance.transform.position;
                disbecharacontroller = (CharacterController)__instance.GetComponent<CharacterController>();
                ladderlayers = __instance.GetComponent<LadderUseScript>().ladderlayers;
                disbeclimbing = __instance.GetComponent<ClimbingPowerupScript>();
                waterlayer = (1 << LayerMask.NameToLayer("WaterLayer")) & ~0x1;
            }

            /* Attempts to stop player from sliding downwards?? */
            __instance.rigidbox.enabled = __instance.CheckGrounded();

            /* Crouching camera and character collider heights */
            float crouchStandSign = __instance.CrouchState ? -1 : 1;
            __instance.currentHeight += Time.deltaTime * __instance.crouchSpeed * crouchStandSign;
            __instance.currentCamHeight += Time.deltaTime * __instance.crouchSpeed * crouchStandSign;
            __instance.currentHeight = Mathf.Clamp(__instance.currentHeight, __instance.crouchHeight, __instance.originalCrouchHeight);
            __instance.currentCamHeight = Mathf.Clamp(__instance.currentCamHeight, __instance.crouchCamHeight, __instance.normalCamHeight);

            disbecharacontroller.height = __instance.currentHeight;
            __instance.dascam.transform.localPosition = new Vector3(0, __instance.currentCamHeight, 0);

            inwater_override = false;
            /*
             * Manual Water Check & Depth Check
             */
            if (__instance.inwater)
            {
                waterlevel = 3.0f;
            }
            else
            {
                Vector3 top = __instance.transform.position + Vector3.up * __instance.currentHeight;
                inwater_override = false;
                RaycastHit hit;
                if (Physics.Raycast(top, -Vector3.up, out hit, __instance.originalCrouchHeight, waterlayer, QueryTriggerInteraction.Collide))
                {
                    inwater_override = true;
                    waterlevel = Mathf.Floor((hit.point - __instance.transform.position).magnitude * 3.0f / __instance.originalCrouchHeight);
                    waterlevel = Mathf.Min(3.0f, waterlevel + 1.0f);
                }
                else
                {
                    waterlevel = 0f;
                }
            }


            GetInput(__instance, __instance.inwater || inwater_override);

            /* Ladder physics code */
            onLadders = false; // Reset ladder status
            Vector3 ladderSpherePos = __instance.transform.position + new Vector3(0, __instance.currentCamHeight, 0);
            Collider[] colliders = Physics.OverlapSphere(ladderSpherePos, disbecharacontroller.radius + 0.075f, ladderlayers);
            foreach (var col in colliders)
            {
                if (col.gameObject.layer == 18)
                    onLadders = true;
            }

            if (disbeclimbing.onwall)
                DoSpidermanMovement(__instance);
            else if ((__instance.inwater || inwater_override) && !onLadders)
                DoWaterMovement(__instance);
            else if (!onLadders && (__instance.CheckGrounded() || __instance.SecondCheckGrounded()))
                DoGroundMovement(__instance);
            else
                DoAirMovement(__instance);

            /* Gravity is added last, helps mitigate issues with ramps */
            if (!onLadders && !disbeclimbing.onwall && !(__instance.inwater || inwater_override))
                velocity.y -= pm_gravity * Time.deltaTime;

            /* Check if the game nudged the character in any way, ie jumppads, explosions, ... */
            velocity.y += __instance.gravityforce * 50.0f * 25.0f;
            velocity += __instance.realrocketjump * 50.0f * 25.0f;
            __instance.realrocketjump = Vector3.zero;
            var velocity_total = velocity / 25.0f;
            __instance.gravityforce = 0;

            /* All done! */
            prev = __instance.transform.position;
            disbecharacontroller.Move(velocity_total * Time.deltaTime);
            return false;
        }
    }

    [HarmonyPatch(typeof(MyControllerScript), "FixedUpdate")]
    class MyControllerScript_FixedUpdate_Patch
    {
        static bool Prefix(MyControllerScript __instance)
        {
            return false;
        }
    }

    [HarmonyPatch(typeof(MyControllerScript), "OnCollisionEnter")]
    [HarmonyPatch(new[] { typeof(Collision) })]
    class MyControllerScript_OnCollisionEnter_Patch
    {
        static bool Prefix(MyControllerScript __instance, Collision hit)
        {
            /*
             * ::TODO::
             * Investigate if it's possible to use this to project velocity vectors into collisions
             * and makeshift limit the velocity on collision so there's a "bump"
             */
            return true;
        }
    }

    [HarmonyPatch(typeof(StatScript), "FixedUpdate")]
    class MyControllerScript_StatScript_Patch
    {
        static void Postfix(StatScript __instance)
        {
            if (disbecontroller)
            {
                __instance.disbesecrets.text =
                    $"\n\n" +
                    $"wl: {waterlevel} \n";
            }
        }
    }
}