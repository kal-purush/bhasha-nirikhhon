namespace SimpleVideoPlayer
{
    class Program : global::Xamarin.Forms.Platform.Tizen.FormsApplication
    {
        protected override void OnCreate()
        {
            base.OnCreate();
            LoadApplication(new App());
        }

        static void Main(string[] args)
        {
            var app = new Program();
            Tizen.TV.UIControls.Forms.UIControls.Init();
            global::Xamarin.Forms.Platform.Tizen.Forms.Init(app);
            Tizen.TV.UIControls.Forms.UIControls.MainWindowProvider = () => app.MainWindow;
            app.Run(args);
        }
    }
}