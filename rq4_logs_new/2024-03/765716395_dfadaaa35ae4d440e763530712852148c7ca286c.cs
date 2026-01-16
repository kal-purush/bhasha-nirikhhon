using Microsoft.AspNetCore.Components;
using EtudeManyToMany.Core.Model;
using EtudeManyToMany.Blazor.Services;
using System.Threading.Tasks;

namespace EtudeManyToMany.Blazor.Pages
{
    public partial class UtilisateurPage : ComponentBase
    {
        private Utilisateur Utilisateur { get; set; }

        [Inject]
        private IService<Utilisateur> UtilisateurService { get; set; }

        private void AddUtilisateur()
        {
            Utilisateur = new Utilisateur();
        }

        private async Task SubmitUtilisateur()
        {
            if (Utilisateur != null)
            {
                bool success = await UtilisateurService.Add(Utilisateur);
                if (success)
                {
                    Utilisateur = null;
                }
            }
        }
    }
}
