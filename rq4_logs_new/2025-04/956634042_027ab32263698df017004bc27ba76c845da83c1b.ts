import i18n from "i18next"
import { initReactI18next } from "react-i18next"

i18n.use(initReactI18next).init({
  lng: navigator.language || "es",
  fallbackLng: "es",
  debug: true,
  resources: {
    en: {
      translation: {
        login: {
          access: "Access",
          username: "Username",
          password: "Password",
          remember: "Remember me",
          forgot: "Forgot my password",
          login: "Log in",
          loginGoogle: "Log in with Google",
          dontHaveAccount: "Don't have an account?",
          register: "Register",
        },
        register: {
          method: {
            title: "Create an account",
            subtitle: "Your information is confidential. We will not share it with anyone.",
            useEmail: "Use email to register",
            useGoogle: "Use Google to register",
            privacyText: "By creating an account, you agree to our",
            privacyLink: "Terms of service and privacy policy",
            haveAccount: "Already have an account?",
            login: "Log in",
          },
          email: {
            title: "Create a perfile with your email",
            subtitle: "Complete the data and receive the registration code by email.",
            nameField: {
              label: "Name",
              placeholder: "Enter your name",
            },
            emailField: {
              label: "Email",
              placeholder: "Complete your email",
              error: {
                required: "The email is required.",
                invalid: "Please enter a valid email address.",
              },
            },
          },
          password: {
            title: "Create a secure password",
            subtitle: "Please create a secure password for your account.",
            passwordField: {
              label: "Password",
              placeholder: "Create a password (minimum 8 characters)",
              error: {
                required: "Please enter the password.",
                invalid: "The password must be at least 8 characters long.",
              },
            },
            passwordStrength: {
              text: "Password strength:",
              weak: "Weak",
              medium: "Medium",
              strong: "Strong",
            },
            repeatPasswordField: {
              label: "Repeat password",
              placeholder: "Repeat your password",
              error: {
                required: "Please repeat the password.",
                mismatch: "The passwords do not match.",
              },
            },
          },
          done: {
            title: "Done",
            subtitle: "Welcome to You are not alone.",
          },
          continue: "Continue",
        },
        "/resources": {
          menu: "Resources",
          title: "Organizations",
        },
        "/profile": {
          menu: "Profile",
          title: "My Profile",
        },
        "/contacts": {
          menu: "Contacts",
          title: "Contacts",
        },
        "/FAQ": {
          menu: "FAQ",
          title: "FAQ",
        },
        "/settings": {
          menu: "Settings",
          title: "Settings",
        },
        exit: {
          menu: "Exit",
          title: "Exit",
        },
        footer: {
          products: "Products",
          support: "Support",
          medicalAssistance: "Medical Assistance",
          resources: "Resources",
          legal: "Legal",
          terms: "Terms",
          conditions: "Conditions",
          privacy: "Privacy",
          contact: "Contact",
          copyright: "©2025 You are not alone - All rights reserved",
          address:
            "YANA S. de R.L. de C.V. Renato Peña 490 Sur, Centro. Monterrey, Nuevo León, México. C.P. 6400",
        },
        header: {
          welcome: "Welcome, {{name}}",
        },
      },
    },
    es: {
      translation: {
        // login
        login: {
          access: "Acceder",
          username: "Usuario",
          password: "Contraseña",
          remember: "Recorderme",
          forgot: "Olvidé la contraseña",
          login: "Ingresar",
          loginGoogle: "Iniciar sesión con Google",
          dontHaveAccount: "¿No tienes una cuenta?",
          register: "Regístrate",
        },

        // register stages
        register: {
          method: {
            title: "Crear una cuenta",
            subtitle: "Tu información es confidencial. No vamos a compartir tus datos.",
            useEmail: "Usar correo electrónico para registrarse",
            useGoogle: "Crear una cuenta con Google",
            privacyText: "Al crear una cuenta, aceptas los",
            privacyLink: "Términos de uso y políticas de privacidad",
            haveAccount: "¿Tienes una cuenta?",
            login: "Ingresar",
          },
          email: {
            title: "Crea un perfil con tu correo electrónico",
            subtitle: "Completa los datos y recibe el código de registro por correo electrónico.",
            nameField: {
              label: "Nombre",
              placeholder: "Ingresa tu nombre",
            },
            emailField: {
              label: "Correo electrónico",
              placeholder: "Completa tu email",
              error: {
                required: "El correo es requerido.",
                invalid: "Por favor, introduce un correo electrónico válido.",
              },
            },
          },
          password: {
            title: "Crea una contraseña segura",
            subtitle: "Por favor, crea una contraseña segura para tu cuenta.",
            passwordField: {
              label: "Contraseña",
              placeholder: "Crea tu contraseña (mínimo 8 caracteres)",
              error: {
                required: "Por favor, introduce la contraseña.",
                invalid: "La contraseña debe tener al menos 8 caracteres.",
              },
            },
            passwordStrength: {
              text: "Seguridad de la contraseña:",
              weak: "Débil",
              medium: "Media",
              strong: "Fuerte",
            },
            repeatPasswordField: {
              label: "Repetir contraseña",
              placeholder: "Repite tu contraseña",
              error: {
                required: "Por favor, repite la contraseña.",
                mismatch: "Las contraseñas no coinciden.",
              },
            },
          },
          done: {
            title: "¡Listo!",
            subtitle: "Te damos la bienvenida a You are not alone.",
          },
          continue: "Continuar",
        },
        "/": {
          menu: "Home",
        },
        "/resources": {
          menu: "Recursos",
          title: "Organizaciones",
        },
        "/profile": {
          menu: "Perfil",
          title: "Mi Perfil",
        },
        "/contacts": {
          menu: "Contactos",
          title: "Contactos",
        },
        "/FAQ": {
          menu: "FAQ",
          title: "FAQ",
        },
        "/settings": {
          menu: "Configuración",
          title: "Configuración",
        },
        exit: {
          menu: "Cerrar sesión",
        },
        footer: {
          products: "Productos",
          support: "Soporte",
          medicalAssistance: "Asistencia médica",
          resources: "Recursos",
          legal: "Legal",
          terms: "Términos",
          conditions: "Condiciones",
          privacy: "Privacidad",
          contact: "Contacto",
          copyright: "©2025 You are not alone - Todos los derechos reservados",
          address:
            "YANA S. de R.L. de C.V. Renato Peña 490 Sur, Centro. Monterrey, Nuevo León, México. C.P. 6400",
        },
        header: {
          welcome: "Bienvenid@, {{name}}",
        },
      },
    },
  },
  interpolation: {
    escapeValue: false,
  },
})

export default i18n