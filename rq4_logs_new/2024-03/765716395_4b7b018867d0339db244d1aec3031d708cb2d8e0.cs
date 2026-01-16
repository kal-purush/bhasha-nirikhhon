using EtudeManyToMany.Blazor.Services;
using EtudeManyToMany.Core.Model;
using Microsoft.AspNetCore.Components;
using System.Threading.Tasks;

namespace EtudeManyToMany.Blazor.Pages
{
    public partial class TrajetPage : ComponentBase
    {
        private Trajet? Trajet { get; set; }

        [Inject]
        private IService<Trajet> TrajetService { get; set; }

        private void AddTrajet()
        {
            Trajet = new Trajet();
        }

        private async Task SubmitTrajet()
        {
            if (Trajet != null)
            {
                bool success = await TrajetService.Add(Trajet);
                if (success)
                {
                    // Trajet ajouté avec succès, réinitialiser le formulaire
                    Trajet = null;
                }
                else
                {
                    // Gérer les erreurs de sauvegarde du trajet
                    // Peut-être afficher un message d'erreur à l'utilisateur
                }
            }
        }
    }
}