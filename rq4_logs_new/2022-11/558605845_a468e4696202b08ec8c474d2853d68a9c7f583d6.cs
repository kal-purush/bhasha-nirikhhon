using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Email;
using Security;
using Validation;

namespace WPFLayer
{
    /// <summary>
    /// Interaction logic for RecoverPasswordPage.xaml
    /// </summary>
    public partial class RecoverPasswordPage : Page, ServicesImplementation.IUsersManagerCallback
    {
        public RecoverPasswordPage()
        {
            InitializeComponent();
            Label_UsernameInvalid.Visibility = Visibility.Hidden;
            Label_EmailInvalid.Visibility = Visibility.Hidden;
        }

        private void Button_RecoverPassword_Click(object sender, RoutedEventArgs e)
        {
            bool validUserName = CheckFieldUserName();
            bool validEmail = CheckFieldEmail();
            if (validUserName && validEmail)
            {
                FindUser();
            }

        }

        private void FindUser() 
        {
            InstanceContext context = new InstanceContext(this);
            ServicesImplementation.UsersManagerClient client = new ServicesImplementation.UsersManagerClient(context);
            ServicesImplementation.Users userLogin = new ServicesImplementation.Users();
            userLogin.username = this.TextBox_Username.Text;
            userLogin.email = this.TextBox_Email.Text;
            bool userFound = client.RecoverPassword(userLogin);
            if (userFound)
            {
                RegisterRecoveryPassword(userLogin, client);
            }
            else
            {
                MessageBox.Show("La información que proporciono no coincide con nuestros registros," +
                " asegurese haberla ingresado correctamente", "Usuario no encontrado", MessageBoxButton.OK, MessageBoxImage.Asterisk);
            }
        }

        private void RegisterRecoveryPassword(ServicesImplementation.Users userLogin, ServicesImplementation.UsersManagerClient client)
        {
            EncryptionService encriptador = new EncryptionService();
            string randomPassword = GenerateRandomPassword();
            userLogin.password = encriptador.StringToSHA512(randomPassword);
            if (client.RegisterRecoveredPassword(userLogin))
            {
                EmailService emailService = new EmailService();
                emailService.SendEmailToUser(userLogin.email, userLogin.username, "Crossword password reset", " Hello! Dear Player, Your new password is: " + 
                "<html><body><h2><center>" +  randomPassword + "</center></h2>" + " PLEASE CHANGE IT AS SOON AS POSSIBLE!!!" + "</body></html>");
                MessageBox.Show("Tu nueva contraseña se envío al correo que ingresaste.", "Contraseña enviada", MessageBoxButton.OK, MessageBoxImage.Information);
                this.NavigationService.GoBack();
            }
        }

        private string GenerateRandomPassword()
        {
            string randomPassword;
            Random random = new Random();
            string securePasswordCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890%$#@*";
            int securePasswordCharacterslength = securePasswordCharacters.Length;
            char passwordCharacter;
            int passwordLength = 10;
            randomPassword = string.Empty;
            for (int i = 0; i < passwordLength; i++)
            {
                passwordCharacter = securePasswordCharacters[random.Next(securePasswordCharacterslength)];
                randomPassword += passwordCharacter.ToString();
            }
            return randomPassword;
        }

        private bool CheckFieldUserName()
        {
            Label_UsernameInvalid.Visibility = Visibility.Hidden;
            this.TextBox_Username.BorderBrush = System.Windows.Media.Brushes.Gray;
            String userName = this.TextBox_Username.Text;
            IUsersValidationService usersValidationService = new UsersValidationService();
            Boolean validUserName = usersValidationService.ValidateUserNameIsNotEmpty(userName);
            if (!validUserName)
            {
                this.TextBox_Username.BorderBrush = System.Windows.Media.Brushes.Red;
                this.Label_UsernameInvalid.Visibility = Visibility.Visible;
            }
            return validUserName;
        }

        private bool CheckFieldEmail()
        {
            Label_EmailInvalid.Visibility = Visibility.Hidden;
            this.TextBox_Email.BorderBrush = System.Windows.Media.Brushes.Gray;
            String email = this.TextBox_Email.Text;
            IUsersValidationService usersValidationService = new UsersValidationService();
            Boolean validPassword = usersValidationService.ValidateEmailFormat(email);
            if (!validPassword)
            {
                this.TextBox_Email.BorderBrush = System.Windows.Media.Brushes.Red;
                this.Label_EmailInvalid.Visibility = Visibility.Visible;
            }
            return validPassword;
        }


        private void Button_Cancel_Click(object sender, RoutedEventArgs e)
        {
            this.NavigationService.GoBack();
        }

        public void Response([MessageParameter(Name = "response")] bool response1)
        {
            throw new NotImplementedException();
        }
    }
}