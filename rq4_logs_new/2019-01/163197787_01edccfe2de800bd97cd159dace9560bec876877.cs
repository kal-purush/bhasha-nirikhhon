using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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

namespace SpamTool_Akhmerov.Controls
{
    public partial class ListControlPanel : UserControl
    {
        #region PanelText : string - Текст на панели

        /// <summary>Текст на панели</summary>
        public static readonly DependencyProperty PanelTextProperty =
            DependencyProperty.Register(
                nameof(PanelText),
                typeof(string),
                typeof(ListControlPanel),
                new PropertyMetadata("Текст панели"));

        /// <summary>Текст на панели</summary>
        public string PanelText
        {
            get => (string)GetValue(PanelTextProperty);
            set => SetValue(PanelTextProperty, value);
        }

        #endregion

        #region ItemSource : IEnumerable - Управляемый список

        /// <summary>Управляемый список</summary>
        public static readonly DependencyProperty ItemSourceProperty =
            DependencyProperty.Register(
                nameof(ItemSource),
                typeof(IEnumerable),
                typeof(ListControlPanel),
                new PropertyMetadata(default(IEnumerable)));

        /// <summary>Управляемый список</summary>
        public IEnumerable ItemSource
        {
            get => (IEnumerable)GetValue(ItemSourceProperty);
            set => SetValue(ItemSourceProperty, value);
        }

        #endregion


        #region ListItemTemplate : DataTemplate - Шаблон отображения данных

        /// <summary>Шаблон отображения данных</summary>
        public static readonly DependencyProperty ListItemTemplateProperty =
            DependencyProperty.Register(
                nameof(ListItemTemplate),
                typeof(DataTemplate),
                typeof(ListControlPanel),
                new PropertyMetadata(default(DataTemplate)));

        /// <summary>Шаблон отображения данных</summary>
        public DataTemplate ListItemTemplate
        {
            get => (DataTemplate)GetValue(ListItemTemplateProperty);
            set => SetValue(ListItemTemplateProperty, value);
        }

        #endregion

        #region SelectedIndex : int - Индекс выбранного элемента

        /// <summary>Индекс выбранного элемента</summary>
        public static readonly DependencyProperty SelectedIndexProperty =
            DependencyProperty.Register(
                nameof(SelectedIndex),
                typeof(int),
                typeof(ListControlPanel),
                new PropertyMetadata(default(int)));

        /// <summary>Индекс выбранного элемента</summary>
        public int SelectedIndex
        {
            get => (int)GetValue(SelectedIndexProperty);
            set => SetValue(SelectedIndexProperty, value);
        }

        #endregion

        #region SelectedItem : object - Выбранный элемент

        /// <summary>Выбранный элемент</summary>
        public static readonly DependencyProperty SelectedItemProperty =
            DependencyProperty.Register(
                nameof(SelectedItem),
                typeof(object),
                typeof(ListControlPanel),
                new PropertyMetadata(default(object)));

        /// <summary>Выбранный элемент</summary>
        public object SelectedItem
        {
            get => (object)GetValue(SelectedItemProperty);
            set => SetValue(SelectedItemProperty, value);
        }

        #endregion

        #region CreateItemCommand : ICommand - Команда создания нового элемента

        /// <summary>Команда создания нового элемента</summary>
        public static readonly DependencyProperty CreateItemCommandProperty =
            DependencyProperty.Register(
                nameof(CreateItemCommand),
                typeof(ICommand),
                typeof(ListControlPanel),
                new PropertyMetadata(default(ICommand)));

        /// <summary>Команда создания нового элемента</summary>
        public ICommand CreateItemCommand
        {
            get => (ICommand)GetValue(CreateItemCommandProperty);
            set => SetValue(CreateItemCommandProperty, value);
        }

        #endregion

        #region RemoveItemCommand : ICommand - Удаление элемента

        /// <summary>Удаление элемента</summary>
        public static readonly DependencyProperty RemoveItemCommandProperty =
            DependencyProperty.Register(
                nameof(RemoveItemCommand),
                typeof(ICommand),
                typeof(ListControlPanel),
                new PropertyMetadata(default(ICommand)));

        /// <summary>Удаление элемента</summary>
        public ICommand RemoveItemCommand
        {
            get => (ICommand)GetValue(RemoveItemCommandProperty);
            set => SetValue(RemoveItemCommandProperty, value);
        }

        #endregion

        #region EditItemCommand : ICommand - Редактирование элемента

        /// <summary>Редактирование элемента</summary>
        public static readonly DependencyProperty EditItemCommandProperty =
            DependencyProperty.Register(
                nameof(EditItemCommand),
                typeof(ICommand),
                typeof(ListControlPanel),
                new PropertyMetadata(default(ICommand)));

        /// <summary>Редактирование элемента</summary>
        public ICommand EditItemCommand
        {
            get => (ICommand)GetValue(EditItemCommandProperty);
            set => SetValue(EditItemCommandProperty, value);
        }

        #endregion



        #region IsEnabled : bool - активация/дезактивация элемента панели 

        public static readonly DependencyProperty IsVisibledProperty = 
            DependencyProperty.Register(
                nameof(IsVisibled),
                typeof(Visibility), 
                typeof(ListControlPanel), 
                new PropertyMetadata(default(Visibility)));

        public Visibility IsVisibled
        {
            get => (Visibility) GetValue(IsVisibledProperty);
            set => SetValue(IsVisibledProperty, value);
        }

        public ListControlPanel() => InitializeComponent();

        #endregion

        public static readonly DependencyProperty ColumnWidthProperty = 
            DependencyProperty.Register(
                nameof(ColumnWidth), 
                typeof(GridLength), 
                typeof(ListControlPanel), 
                new PropertyMetadata(default(GridLength)));

        public GridLength ColumnWidth
        {
            get => (GridLength) GetValue(ColumnWidthProperty);
            set => SetValue(ColumnWidthProperty, value);
        }

        public static readonly DependencyProperty ControlWidthProperty = 
            DependencyProperty.Register(
                nameof(ControlWidth), 
                typeof(GridLength), 
                typeof(ListControlPanel), 
                new PropertyMetadata(default(GridLength)));

        public GridLength ControlWidth
        {
            get => (GridLength) GetValue(ControlWidthProperty);
            set => SetValue(ControlWidthProperty, value);
        }
    }
}