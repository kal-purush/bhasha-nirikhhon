import { Navigate, useLocation } from "react-router-dom";
import useAuth from "../../../../hooks/useAuth";

function GuestRoute({ children }) {
  const auth = useAuth();
  const location = useLocation();

  const fromPage = location.state?.from?.pathname || "/";
  //if user exists redirect him to the page he came from because this page is only for guests
  return auth.user ? <Navigate to={fromPage} replace /> : children;
}

export default GuestRoute;