var http =  require ( ' http ' );
http . createServer ( fonction ( req , res ) {
   res . writeHead ( 200 , { ' Content-Type ' :  ' text / plain ' });
   res . end ( ' Bonjour Travis! \ n ' )
}). écouter ( 1337 , ' 127.0.0.1 ' );
console . log ( ' Serveur fonctionnant à http://127.0.0.1:1337/ ' );