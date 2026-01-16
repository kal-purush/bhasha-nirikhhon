using System;
using System.Windows;
using Microsoft.Practices.Unity;
using Prism.Unity;
using YASM.Views;

namespace YASM
{
	/// <summary>
	/// Description of Bootstrapper.
	/// </summary>
	public class Bootstrapper : UnityBootstrapper
	{
		#region implemented abstract members of Bootstrapper
		protected override DependencyObject CreateShell()
		{
			return Container.Resolve<MainWindow>();
		}
		#endregion
		
		protected override void InitializeShell()
		{
			Application.Current.MainWindow.Show();
		}
		
		protected override void ConfigureContainer()
		{
			base.ConfigureContainer();
			
			Container.RegisterType<>();
		}
	}
}