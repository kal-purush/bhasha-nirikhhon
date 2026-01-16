import database.ConnexionBdd;
import java.io.IOException;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

@WebServlet("/connexion")
public class ServletConnexion extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // Récupérer les paramètres d'authentification (email et mot de passe)
        String email = request.getParameter("mail");
        String password = request.getParameter("pass");

        // Votre logique de vérification d'authentification avec la base de données
        boolean authentifie = ConnexionBdd.verifierAuthentification(email, password);

        if (authentifie) {
            // Si l'authentification réussit, établir une session
            HttpSession session = request.getSession();
            session.setAttribute("utilisateurConnecte", email);
            
            // Récupérer le nom et le prénom du pompier à partir de la base de données
            String nomPrenomPompier = ConnexionBdd.recupererNomPrenomPompier(email);
            session.setAttribute("nomPrenomPompier", nomPrenomPompier); // Stocker le nom et le prénom du pompier dans la session

            // Redirection vers la servlet de redirection après connexion réussie
            response.sendRedirect(request.getContextPath() + "/ServletPompier/lister");
        } else {
            // Si l'authentification échoue, rediriger vers la page de connexion avec un message d'erreur
            response.sendRedirect(request.getContextPath() + "/index.jsp");
        }
    }
}