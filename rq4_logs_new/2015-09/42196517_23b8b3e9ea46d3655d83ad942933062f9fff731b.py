#!/usr/bin/python
#-=- encoding: utf-8 -=-

# Abaixo defina os campos do banco de dados
def acesso():
	imap 		='imap.dominio.com.br'
        imaplogin	='mailflow'
	imapsenha	='senha'
	smtp		='smtp.gmail.com'
	smtpporta	='465'
	smtplogin 	='login'
	smtpmail	='login@gmail.com'
	smtpsenha 	='senha'

	return imap, imaplogin, imapsenha, smtp, smtpporta, smtplogin, smtpmail, smtpsenha
