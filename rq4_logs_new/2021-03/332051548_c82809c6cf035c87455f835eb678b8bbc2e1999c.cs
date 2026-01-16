using System;
using Order.Shared.Security.Constants;

namespace Order.Client.Constants
{
    public static class UIMessages
    {
        // Http default error messages. To use if the Http error messages beneath are null.
        public static string DefaultHttpNotFoundError { get => "La ressource demandée n'a pas été trouvée"; }
        public static string DefaultHttpBadRequestError { get => "La demande n'a pas pu être construit correctement"; }
        public static string DefaultHttpUnauthorizedError { get => "Vous n'êtes pas autorisé à accéder à la ressource demandée. Vous reconnecter peut résoudre le problème"; }
        public static string DefaultHttpServerError { get => "Une erreur s'est produite sur le serveur. Veuillez réessayer ou contactez le support"; }
        public static string DefaultHttpRequestTimedOut { get => "Un problème de connexion est survenu. Veuillez vérifier votre réseau ou votre connexion internet"; }
        public static string DefaultInternalError { get => "Une erreur interne est survenue. Veuillez réessayer ou contacter le support"; }

        public static string SignUpSuccess { get => "Veuillez suivre le lien reçu par email pour finaliser votre inscription"; }

        public static string Email { get => "Email"; }
        public static string Password { get => "Mot de passe"; }
        public static string ConfirmPassword { get => "Confirmer mot de passe"; }
        public static string ForgotPassword { get => "Mot de passe oublié?"; }

        public static string SignIn { get => "Connexion"; }
        public static string SignUp { get => "Inscription"; }

        public static string Login { get => "Se connecter"; }
        public static string Register { get => "S'inscrire"; }

        public static string AskForAccountExistance { get => "Vous avez déja un compte? "; }
        public static string SignInRedirect { get => "Connectez vous"; }
        public static string AskForAccountAbsence { get => "Vous n'avez pas de compte?"; }
        public static string SignUpRedirect { get => "Rejoignez nous"; }

        public static string ContinueWith { get => "Continuer avec"; }

        public static string LastName { get => "Nom"; }
        public static string FirstName { get => "Prénom"; }

        public static string EmailAlreadyHasAccount { get => "L'email fourni est déjà associé à un compte. Considérez de vous connecter avec ce compte ou de fournir un autre email"; }
        public static string InvalidEmailAdress { get => "L'email fourni n'est pas valide. Veuillez fournir une adresse email valide"; }
        public static string PasswordNotSecure { get => "Le mot de passe ne répond pas aux exigences de sécurité. veuillez envisager de l'allonger ou d'utiliser des caractères spéciaux"; }
        public static string PasswordMismatch { get => "Le mot de passe et sa confirmation ne correspondent pas. Veuillez saisir à nouveau le mot de passe."; }
        public static string DefaultSignUpErrorMessage { get => "Ouups! Il semble que le compte n'a pas pu être créé. Veuillez réessayer plus tard"; }

        public static string EmailNotConfirmed { get => "Votre email n'a pas été confirmé. Veuillez suivre le lien reçu par email pour confirmer l'adresse"; }
        public static string AccountLockedOut(DateTimeOffset? remainning) => $"Votre compte est bloqué car {UserConstants.MAX_FAILED_SIGNIN} tentatives infructueuses de connexion. Veuillez réessayer dans {remainning.Value.ToLocalTime().Subtract(DateTime.Now).Minutes + 1} minutes";
        public static string WrongEmailOrPassword { get => "Email ou mot de passe incorrect, veuillez réessayer"; }
        public static string DefaultSignInErrorMessage { get => "impossible de vous connecter. Veuillez envisager de créer un compte ou de continuer avec un compte de réseau social"; }

        public static string Send { get => "Envoyer"; }
        public static string Cancel { get => "Annuler"; }

        public static string Reset { get => "Réinitialiser"; }
        public static string DefaultResetPasswordFailed { get => "Ouups! Il semple que le mot de passe n'a pas pu être réinitialiser.  Veuillez réessayer plus tard"; }

        public static string ComeBackTo { get => "Revenir à"; }
        public static string SignInPage { get => "la page de connexion"; }
        public static string CannotRequestResetPassword { get => "Ouups! Impossible de demander la récupération du mot de passe. Veuillez réessayer ultérieurement."; }
        public static string FollowResetPasswordLink { get => "Veuillez suivre le lien que vous avez reçu par e-mail pour réinitialiser votre mot de passe."; }
        public static string ResetPasswordsuccess { get => "Votre mot de passe a été réinitialisé avec succès"; }

        public static string CannotSignInWithSocialProvider(string provider) => $"Ouups! Impossible de se connecter avec {provider}. Veuillez réessayer ultérieurement.";
    }
}