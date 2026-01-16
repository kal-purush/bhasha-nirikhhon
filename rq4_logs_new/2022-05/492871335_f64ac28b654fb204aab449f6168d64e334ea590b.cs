// GENERATED AUTOMATICALLY FROM 'Assets/Inputs/Controles.inputactions'

using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine.InputSystem;
using UnityEngine.InputSystem.Utilities;

public class @Controles : IInputActionCollection, IDisposable
{
    public InputActionAsset asset { get; }
    public @Controles()
    {
        asset = InputActionAsset.FromJson(@"{
    ""name"": ""Controles"",
    ""maps"": [
        {
            ""name"": ""Jugador"",
            ""id"": ""08fe4a20-b313-4d75-bf25-039656c6988d"",
            ""actions"": [
                {
                    ""name"": ""Mover"",
                    ""type"": ""Value"",
                    ""id"": ""1c2ce463-ed4d-485a-8ca4-cadb5f5a7321"",
                    ""expectedControlType"": ""Vector2"",
                    ""processors"": """",
                    ""interactions"": """"
                },
                {
                    ""name"": ""Saltar"",
                    ""type"": ""Button"",
                    ""id"": ""5f1250b8-aaf6-4e89-b348-5fb3b872d3c7"",
                    ""expectedControlType"": ""Button"",
                    ""processors"": """",
                    ""interactions"": """"
                },
                {
                    ""name"": ""Mirar"",
                    ""type"": ""PassThrough"",
                    ""id"": ""7638974e-654c-4ee2-acec-8164a99f114a"",
                    ""expectedControlType"": ""Vector2"",
                    ""processors"": """",
                    ""interactions"": """"
                },
                {
                    ""name"": ""Interaccion"",
                    ""type"": ""Button"",
                    ""id"": ""4de54dfd-190b-402d-94f5-cca8ab35970f"",
                    ""expectedControlType"": ""Button"",
                    ""processors"": """",
                    ""interactions"": """"
                }
            ],
            ""bindings"": [
                {
                    ""name"": ""WASD"",
                    ""id"": ""00ca640b-d935-4593-8157-c05846ea39b3"",
                    ""path"": ""Dpad"",
                    ""interactions"": """",
                    ""processors"": """",
                    ""groups"": """",
                    ""action"": ""Mover"",
                    ""isComposite"": true,
                    ""isPartOfComposite"": false
                },
                {
                    ""name"": ""up"",
                    ""id"": ""e2062cb9-1b15-46a2-838c-2f8d72a0bdd9"",
                    ""path"": ""<Keyboard>/w"",
                    ""interactions"": """",
                    ""processors"": """",
                    ""groups"": "";Keyboard&Mouse"",
                    ""action"": ""Mover"",
                    ""isComposite"": false,
                    ""isPartOfComposite"": true
                },
                {
                    ""name"": ""down"",
                    ""id"": ""320bffee-a40b-4347-ac70-c210eb8bc73a"",
                    ""path"": ""<Keyboard>/s"",
                    ""interactions"": """",
                    ""processors"": """",
                    ""groups"": "";Keyboard&Mouse"",
                    ""action"": ""Mover"",
                    ""isComposite"": false,
                    ""isPartOfComposite"": true
                },
                {
                    ""name"": ""left"",
                    ""id"": ""d2581a9b-1d11-4566-b27d-b92aff5fabbc"",
                    ""path"": ""<Keyboard>/a"",
                    ""interactions"": """",
                    ""processors"": """",
                    ""groups"": "";Keyboard&Mouse"",
                    ""action"": ""Mover"",
                    ""isComposite"": false,
                    ""isPartOfComposite"": true
                },
                {
                    ""name"": ""right"",
                    ""id"": ""fcfe95b8-67b9-4526-84b5-5d0bc98d6400"",
                    ""path"": ""<Keyboard>/d"",
                    ""interactions"": """",
                    ""processors"": """",
                    ""groups"": "";Keyboard&Mouse"",
                    ""action"": ""Mover"",
                    ""isComposite"": false,
                    ""isPartOfComposite"": true
                },
                {
                    ""name"": """",
                    ""id"": ""3b58685b-4dd0-41f4-8758-798ac825d458"",
                    ""path"": ""<Keyboard>/space"",
                    ""interactions"": """",
                    ""processors"": """",
                    ""groups"": ""Keyboard&Mouse"",
                    ""action"": ""Saltar"",
                    ""isComposite"": false,
                    ""isPartOfComposite"": false
                },
                {
                    ""name"": """",
                    ""id"": ""36b387cd-508f-4c12-abbb-4d6458bb1a98"",
                    ""path"": ""<Mouse>/delta"",
                    ""interactions"": """",
                    ""processors"": """",
                    ""groups"": """",
                    ""action"": ""Mirar"",
                    ""isComposite"": false,
                    ""isPartOfComposite"": false
                },
                {
                    ""name"": """",
                    ""id"": ""a829d3fd-923a-424c-8487-5f3a77e741dc"",
                    ""path"": ""<Keyboard>/e"",
                    ""interactions"": """",
                    ""processors"": """",
                    ""groups"": ""Keyboard&Mouse"",
                    ""action"": ""Interaccion"",
                    ""isComposite"": false,
                    ""isPartOfComposite"": false
                }
            ]
        }
    ],
    ""controlSchemes"": [
        {
            ""name"": ""Keyboard&Mouse"",
            ""bindingGroup"": ""Keyboard&Mouse"",
            ""devices"": [
                {
                    ""devicePath"": ""<Keyboard>"",
                    ""isOptional"": false,
                    ""isOR"": false
                },
                {
                    ""devicePath"": ""<Mouse>"",
                    ""isOptional"": false,
                    ""isOR"": false
                }
            ]
        },
        {
            ""name"": ""Gamepad"",
            ""bindingGroup"": ""Gamepad"",
            ""devices"": [
                {
                    ""devicePath"": ""<Gamepad>"",
                    ""isOptional"": false,
                    ""isOR"": false
                }
            ]
        },
        {
            ""name"": ""Touch"",
            ""bindingGroup"": ""Touch"",
            ""devices"": [
                {
                    ""devicePath"": ""<Touchscreen>"",
                    ""isOptional"": false,
                    ""isOR"": false
                }
            ]
        },
        {
            ""name"": ""Joystick"",
            ""bindingGroup"": ""Joystick"",
            ""devices"": [
                {
                    ""devicePath"": ""<Joystick>"",
                    ""isOptional"": false,
                    ""isOR"": false
                }
            ]
        },
        {
            ""name"": ""XR"",
            ""bindingGroup"": ""XR"",
            ""devices"": [
                {
                    ""devicePath"": ""<XRController>"",
                    ""isOptional"": false,
                    ""isOR"": false
                }
            ]
        }
    ]
}");
        // Jugador
        m_Jugador = asset.FindActionMap("Jugador", throwIfNotFound: true);
        m_Jugador_Mover = m_Jugador.FindAction("Mover", throwIfNotFound: true);
        m_Jugador_Saltar = m_Jugador.FindAction("Saltar", throwIfNotFound: true);
        m_Jugador_Mirar = m_Jugador.FindAction("Mirar", throwIfNotFound: true);
        m_Jugador_Interaccion = m_Jugador.FindAction("Interaccion", throwIfNotFound: true);
    }

    public void Dispose()
    {
        UnityEngine.Object.Destroy(asset);
    }

    public InputBinding? bindingMask
    {
        get => asset.bindingMask;
        set => asset.bindingMask = value;
    }

    public ReadOnlyArray<InputDevice>? devices
    {
        get => asset.devices;
        set => asset.devices = value;
    }

    public ReadOnlyArray<InputControlScheme> controlSchemes => asset.controlSchemes;

    public bool Contains(InputAction action)
    {
        return asset.Contains(action);
    }

    public IEnumerator<InputAction> GetEnumerator()
    {
        return asset.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    public void Enable()
    {
        asset.Enable();
    }

    public void Disable()
    {
        asset.Disable();
    }

    // Jugador
    private readonly InputActionMap m_Jugador;
    private IJugadorActions m_JugadorActionsCallbackInterface;
    private readonly InputAction m_Jugador_Mover;
    private readonly InputAction m_Jugador_Saltar;
    private readonly InputAction m_Jugador_Mirar;
    private readonly InputAction m_Jugador_Interaccion;
    public struct JugadorActions
    {
        private @Controles m_Wrapper;
        public JugadorActions(@Controles wrapper) { m_Wrapper = wrapper; }
        public InputAction @Mover => m_Wrapper.m_Jugador_Mover;
        public InputAction @Saltar => m_Wrapper.m_Jugador_Saltar;
        public InputAction @Mirar => m_Wrapper.m_Jugador_Mirar;
        public InputAction @Interaccion => m_Wrapper.m_Jugador_Interaccion;
        public InputActionMap Get() { return m_Wrapper.m_Jugador; }
        public void Enable() { Get().Enable(); }
        public void Disable() { Get().Disable(); }
        public bool enabled => Get().enabled;
        public static implicit operator InputActionMap(JugadorActions set) { return set.Get(); }
        public void SetCallbacks(IJugadorActions instance)
        {
            if (m_Wrapper.m_JugadorActionsCallbackInterface != null)
            {
                @Mover.started -= m_Wrapper.m_JugadorActionsCallbackInterface.OnMover;
                @Mover.performed -= m_Wrapper.m_JugadorActionsCallbackInterface.OnMover;
                @Mover.canceled -= m_Wrapper.m_JugadorActionsCallbackInterface.OnMover;
                @Saltar.started -= m_Wrapper.m_JugadorActionsCallbackInterface.OnSaltar;
                @Saltar.performed -= m_Wrapper.m_JugadorActionsCallbackInterface.OnSaltar;
                @Saltar.canceled -= m_Wrapper.m_JugadorActionsCallbackInterface.OnSaltar;
                @Mirar.started -= m_Wrapper.m_JugadorActionsCallbackInterface.OnMirar;
                @Mirar.performed -= m_Wrapper.m_JugadorActionsCallbackInterface.OnMirar;
                @Mirar.canceled -= m_Wrapper.m_JugadorActionsCallbackInterface.OnMirar;
                @Interaccion.started -= m_Wrapper.m_JugadorActionsCallbackInterface.OnInteraccion;
                @Interaccion.performed -= m_Wrapper.m_JugadorActionsCallbackInterface.OnInteraccion;
                @Interaccion.canceled -= m_Wrapper.m_JugadorActionsCallbackInterface.OnInteraccion;
            }
            m_Wrapper.m_JugadorActionsCallbackInterface = instance;
            if (instance != null)
            {
                @Mover.started += instance.OnMover;
                @Mover.performed += instance.OnMover;
                @Mover.canceled += instance.OnMover;
                @Saltar.started += instance.OnSaltar;
                @Saltar.performed += instance.OnSaltar;
                @Saltar.canceled += instance.OnSaltar;
                @Mirar.started += instance.OnMirar;
                @Mirar.performed += instance.OnMirar;
                @Mirar.canceled += instance.OnMirar;
                @Interaccion.started += instance.OnInteraccion;
                @Interaccion.performed += instance.OnInteraccion;
                @Interaccion.canceled += instance.OnInteraccion;
            }
        }
    }
    public JugadorActions @Jugador => new JugadorActions(this);
    private int m_KeyboardMouseSchemeIndex = -1;
    public InputControlScheme KeyboardMouseScheme
    {
        get
        {
            if (m_KeyboardMouseSchemeIndex == -1) m_KeyboardMouseSchemeIndex = asset.FindControlSchemeIndex("Keyboard&Mouse");
            return asset.controlSchemes[m_KeyboardMouseSchemeIndex];
        }
    }
    private int m_GamepadSchemeIndex = -1;
    public InputControlScheme GamepadScheme
    {
        get
        {
            if (m_GamepadSchemeIndex == -1) m_GamepadSchemeIndex = asset.FindControlSchemeIndex("Gamepad");
            return asset.controlSchemes[m_GamepadSchemeIndex];
        }
    }
    private int m_TouchSchemeIndex = -1;
    public InputControlScheme TouchScheme
    {
        get
        {
            if (m_TouchSchemeIndex == -1) m_TouchSchemeIndex = asset.FindControlSchemeIndex("Touch");
            return asset.controlSchemes[m_TouchSchemeIndex];
        }
    }
    private int m_JoystickSchemeIndex = -1;
    public InputControlScheme JoystickScheme
    {
        get
        {
            if (m_JoystickSchemeIndex == -1) m_JoystickSchemeIndex = asset.FindControlSchemeIndex("Joystick");
            return asset.controlSchemes[m_JoystickSchemeIndex];
        }
    }
    private int m_XRSchemeIndex = -1;
    public InputControlScheme XRScheme
    {
        get
        {
            if (m_XRSchemeIndex == -1) m_XRSchemeIndex = asset.FindControlSchemeIndex("XR");
            return asset.controlSchemes[m_XRSchemeIndex];
        }
    }
    public interface IJugadorActions
    {
        void OnMover(InputAction.CallbackContext context);
        void OnSaltar(InputAction.CallbackContext context);
        void OnMirar(InputAction.CallbackContext context);
        void OnInteraccion(InputAction.CallbackContext context);
    }
}