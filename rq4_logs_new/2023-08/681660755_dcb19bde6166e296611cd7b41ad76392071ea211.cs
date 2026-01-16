using Business.Abstract;
using Business.Concrete;
using System;
using System.Collections.Generic;
using System.Text;
using Business.DependencyResolvers.Autofac;
using System.ComponentModel;
using Autofac;
using DataAccess.Abstract;
using Autofac.Extras.DynamicProxy;
using Core.Utilities.Interceptors;
using Core.Utilities.Security.JWT;
using Castle.DynamicProxy;

namespace Business.DependencyResolvers.Autofac
{
    public class AutofacBusinessModule : Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterType<ReceiptManager>().As<IReceiptService>().SingleInstance();
        }
    }
}