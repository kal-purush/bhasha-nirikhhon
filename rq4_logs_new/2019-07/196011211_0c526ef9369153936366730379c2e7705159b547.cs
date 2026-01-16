using System;
using System.Collections.Generic;
using System.Text;

namespace RuimDIP
{
    class Mensagens
    {
        public void EnvioDeMensagens()
        {
            var mensagemSkype = new EnviarMensagemSkype();
            var mensagemSlack = new EnviarMensagemSlack();

            mensagemSkype.EnviarMensagem();
            mensagemSlack.EnviarMensagem();
        }
    }

    class EnviarMensagemSkype
    {
        public void EnviarMensagem() { }
    }

    class EnviarMensagemSlack
    {
        public void EnviarMensagem() { }
    }
}

namespace BomDIP
{
    class Mensagens
    {
        private IEnviarMensagem _enviarMensagem;

        public Mensagens(IEnviarMensagem enviarMensagem)
        {
            _enviarMensagem = enviarMensagem;
        }

        public void EnvioDeMensagens()
        {
            _enviarMensagem.EnviarMensagem();
        }
    }

    interface IEnviarMensagem
    {
        void EnviarMensagem();
    }

    class EnviarMensagemSkype : IEnviarMensagem
    {
        public void EnviarMensagem() { }
    }

    class EnviarMensagemSlack : IEnviarMensagem
    {
        public void EnviarMensagem() { }
    }   
}