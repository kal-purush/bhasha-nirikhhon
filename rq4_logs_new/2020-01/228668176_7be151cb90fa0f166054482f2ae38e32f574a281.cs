/*
Copyright © 2017-2019 César Andrés Morgan
Licenciado para uso interno solamente.
*/

using System;
using System.Collections.Generic;
using TheXDS.MCART.PluginSupport.Legacy;
using TheXDS.Proteus.Crud;
using TheXDS.Proteus.ViewModels.Base;
using TheXDS.Proteus.Widgets;
using TheXDS.MCART.Types.Extensions;
using static TheXDS.MCART.ReflectionHelpers;

namespace TheXDS.Proteus.Plugins
{
    [Flags]
    public enum CrudToolVisibility: byte
    {
        Unselected = 1,
        Selected,
        NotEditing,
        Editing,
        EditAndUnselected,
        EditAndSelected,
        Everywhere
    }

    public abstract class CrudTool : WpfPlugin
    {
        public virtual bool Available(IEnumerable<Type> models) => true;
        public virtual bool Available(Type model) => Available(new[] { model });

        public CrudToolVisibility Visibility { get; }
        protected CrudTool(CrudToolVisibility visibility)
        {
            Visibility = visibility;
        }

        public abstract IEnumerable<Launcher> GetLaunchers(IEnumerable<Type> models);
        public virtual IEnumerable<Launcher> GetLaunchers(Type model) => GetLaunchers(new[] { model });
    }

    public class NewEntityCrudTool : CrudTool
    {
        public NewEntityCrudTool() : base(CrudToolVisibility.Unselected)
        {
        }

        public override IEnumerable<Launcher> GetLaunchers(IEnumerable<Type> models)
        {
            foreach (var j in models)
            {
                var d = CrudElement.GetDescription(j)?.FriendlyName ?? j.Name;
                var oc = GetMethod<CrudViewModelBase, Action<object>>(p => p.OnCreate);



                yield return new Launcher(
                    $"Nuevo {d}",
                    $"Crea un nuevo {d}",
                    oc.FullName(),
                    new ObservingCommand(vm, vm);
            }
        }
    }
}