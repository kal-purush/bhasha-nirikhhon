import { useCallback, useState } from "react";
import { Alert } from "react-native";
import useUserToken from "./useUserToken"

const BASE_API_ENDPOINT = process.env.BASE_API_ENDPOINT;

const useFetchStudentType = () => {
  const { userToken } = useUserToken();
  const [fetchedStudentType, setFetchStudentType] = useState(null);

  // "student types" endpoint
  const studentTypesEndpoint = `${BASE_API_ENDPOINT}/school/student-types`;

  const checkStudentType = useCallback(
    async (studentTypeId) => {
      try {
        const serverResponse = await fetch(`${studentTypesEndpoint}/${studentTypeId}/`, {
          method: 'GET',
          headers: {
            Authorization: `Token ${userToken}`
          }
        });

        const responseJson = await serverResponse.json();
        setFetchStudentType(responseJson)

        return responseJson;
      } catch (e) {
        // 'Network request failed' is thrown if the server is unreachable
        if (e.message === 'Network request failed') {
          Alert.alert(
            'Error al obtener la facultad',
            'El servidor está en mantenimiento. Vuelve más tarde.'
          );
        } else {
          Alert.alert('Error al obtener la facultad', `${e}`);
        }
      }
    },
    [userToken]
  )

  return {
    fetchedStudentType,
    checkStudentType
  }
}

export default useFetchStudentType