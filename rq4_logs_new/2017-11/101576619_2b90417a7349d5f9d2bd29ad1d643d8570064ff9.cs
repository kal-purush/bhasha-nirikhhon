using Android.App;
using Android.Widget;
using Android.OS;

namespace DIMO
{
    [Activity(Label = "Teste", MainLauncher = true, Icon = "@drawable/icon")]
    public class MainActivity : Activity
    {
        protected override void OnCreate(Bundle bundle)
        {
            base.OnCreate(bundle);

            Button button1 = FindViewById<Button>(Resource.Id.btnListarTurmas);
            button1.Click += (sender, e) => {
                
            };

            Button button2 = FindViewById<Button>(Resource.Id.btnCadastrarTurma);
            button2.Click += (sender, e) => {

            };
            
        }
    }
}
