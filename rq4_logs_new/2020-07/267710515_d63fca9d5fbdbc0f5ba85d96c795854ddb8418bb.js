import * as types from "../constants/ActionTypes";

export function GetEvenements() {
  return function (dispatch) {
    return fetch("http://testhadrienback/api/evenements/list.php")
      .then((response) => response.json())
      .then((json) => {
        console.log(json);
        dispatch({
          type: types.EVENEMENT_DATA_LOADED,
          payload: json.evenementList,
        });
      });
  };
}

export function GetNextEvenement() {
  return function (dispatch) {
    return fetch("http://testhadrienback/api/evenements/getNextEvenement.php")
      .then((response) => response.json())
      .then((json) => {
        console.log(json);
        dispatch({
          type: types.EVENEMENT_DATA_LOADED,
          payload: json.evenementList,
        });
      });
  };
}


  export function ReactOnEvenement(evenement, auth_token) {
  return function (dispatch) {
    return fetch("http://testhadrienback/api/evenements/StatutSeen.php", {
      method: "post",
      body: JSON.stringify({
        id: evenement.id,
        auth_token: auth_token,
      }),
    })
        .then((response) => {
        if (response.status === 200 || response.status === 201) {
            dispatch(GetNextEvenement());
          } else {
              dispatch({
                type: types.EVENEMENT_ADD_ERROR,
            });
          }
        
       })
       .catch(() => {
            dispatch({
                type: types.EVENEMENT_ADD_ERROR,
        });
      });
  };
}

export function AddEvenement(evenement) {
  return function (dispatch) {
    return fetch("http://testhadrienback/api/evenements/add.php", {
      method: "post",
      body: JSON.stringify(evenement),
    })
      .then((response) => {
          if (response.status === 200 || response.status === 201) {
             response.json().then((json) => {
            dispatch({
              type: types.EVENEMENT_ADD,
              payload: json,
            });
            dispatch({
              type: types.REDIRECT,
              payload: "/Accueil",
             });       
          });
        } else {
          dispatch({
            type: types.EVENEMENT_ADD_ERROR,
          });
        }
      })
      .catch(() => {
        dispatch({
          type: types.EVENEMENT_ADD_ERROR,
        });
      });
  };


  
}