import { apiUrl } from "../lib/apiconfigs/apiUrl";
import { storeInteractor } from "../lib/localStorage/storeInteractor";

export async function deleteContainer(containerId: number){
  const headers = new Headers();
  headers.set('Content-Type', 'application/json');
  headers.set('Authorization', storeInteractor.getToken());
    if (window.confirm('Are you sure you want to delete this container?')) {
      await fetch ( apiUrl(`/container?cid=${containerId}`),{method: 'DELETE', headers: headers})
      .then(response => {
        console.log("Delete Successful", response)
      })
      .catch(error => {
        console.log(error)
      })
    }
  }




