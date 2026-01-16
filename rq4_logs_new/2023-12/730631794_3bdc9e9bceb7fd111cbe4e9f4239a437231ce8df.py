    # @property
    # def service(self):
    #    return self.service
    #
for sal in liste:
    service = sal.service
    if service not in compte_service:
        compte_service[service]=1
    else:
        compte_service[service]+=1
# for essai in enumerate(liste):
#     # print(essai[1].service)
#     liste_service.append(essai[1].service)
# print(liste_service)
# for service in liste_service:
#     # print(service, liste_service.count(service))
#     compte_service.update({service: liste_service.count(service)})
# print(f"---------------------Dictionnaire service---------------------")
print(compte_service)
class Personne:
   def __init__(self, nom, age):
        self.nom = nom
        self.age = age


   def __eq__(self, other):
        if isinstance(other, Personne):
            return self.nom == other.nom and self.age == other.age
        return False


personne1 = Personne("Alice", 25)
personne2 = Personne("Alice", 25)


resultat_apres_redefinition = personne1 == personne2
print(f"Après redéfinition: Les instances sont-elles égales? {resultat_apres_redefinition}")