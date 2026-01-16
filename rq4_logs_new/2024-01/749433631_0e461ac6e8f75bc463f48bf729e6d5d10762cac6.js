$("#new_edit_utilisateur").on('submit', function(){
  if($("#user_password").val() != $("#verifpass").val()) {
      //implémntez votre code
      alert("Les deux mots de passe saisies sont différents");
      alert("Merci de renouveler l'opération");
      return false;
  }
})