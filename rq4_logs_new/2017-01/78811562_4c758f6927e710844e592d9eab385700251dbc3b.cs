using NewellClark.ViewModels;
using NewellClark.Wpf.UserControls.TypeConverters;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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

namespace NewellClark.Wpf.UserControls.Views
{
	/// <summary>
	/// Interaction logic for MultiLineRegexInput.xaml
	/// </summary>
	public partial class MultiLineRegexInput : UserControl
	{
		public MultiLineRegexInput()
		{
			_textBrushConverter = new BooleanToToggleConverter<Brush>(
				new SolidColorBrush(Colors.Black),
				new SolidColorBrush(Colors.Red));

			InitializeComponent();

			_viewModel = new RegexViewModel();
			pattern.DataContext = _viewModel;
			optionsSelector.DataContext = _viewModel;

			_validTextBrush = new DesignerOnlyProperty<Brush>(
				this, () => _textBrushConverter.True, x => _textBrushConverter.True = x);
			_invalidTextBrush = new DesignerOnlyProperty<Brush>(
				this, () => _textBrushConverter.False, x => _textBrushConverter.False = x);

			_viewModel.PropertyChanged += (o, e) =>
			{
				if (e.PropertyName == nameof(_viewModel.IsValid))
				{
					IsValid = _viewModel.IsValid;
				}
				if (e.PropertyName == nameof(_viewModel.Regex))
				{
					Regex = _viewModel.Regex;
				}
			};

			var textColorBinding = new Binding(nameof(_viewModel.IsValid));
			textColorBinding.Converter = _textBrushConverter;
			textColorBinding.Mode = BindingMode.OneWay;
			textColorBinding.UpdateSourceTrigger = UpdateSourceTrigger.PropertyChanged;
			pattern.SetBinding(TextBox.ForegroundProperty, textColorBinding);
		}

		[EditorBrowsable(EditorBrowsableState.Never)]
		public Regex Regex
		{
			get { return (Regex)GetValue(RegexProperty); }
			set { SetValue(RegexProperty, value); }
		}
		private static void HandleRegexPropertyChanged(object sender, DependencyPropertyChangedEventArgs e)
		{
			((MultiLineRegexInput)sender)._viewModel.Regex = (Regex)e.NewValue;
		}
		public static DependencyProperty RegexProperty = DependencyProperty.Register(
			nameof(Regex), typeof(Regex), typeof(MultiLineRegexInput), new PropertyMetadata(HandleRegexPropertyChanged));

		[EditorBrowsable(EditorBrowsableState.Never)]
		public bool IsValid
		{
			get { return (bool)GetValue(_isValidPropertyKey.DependencyProperty); }
			private set { SetValue(_isValidPropertyKey, value); }
		}
		private static readonly DependencyPropertyKey _isValidPropertyKey = DependencyProperty.RegisterReadOnly(
			nameof(IsValid), typeof(bool), typeof(MultiLineRegexInput), new PropertyMetadata(false));

		public Brush ValidTextBrush
		{
			get { return _validTextBrush.Get(); }
			set { _validTextBrush.Set(value); }
		}
		private DesignerOnlyProperty<Brush> _validTextBrush;

		public Brush InvalidTextBrush
		{
			get { return _validTextBrush.Get(); }
			set { _validTextBrush.Set(value); }
		}
		private DesignerOnlyProperty<Brush> _invalidTextBrush;

		private RegexViewModel _viewModel;
		private BooleanToToggleConverter<Brush> _textBrushConverter;
	}
}