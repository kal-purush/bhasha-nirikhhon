import {User} from "firebase/auth"
import {getIngredientsByUser} from "FirebaseApi/database"
import {useEffect, useState} from "react"
import {useDispatch} from "react-redux"
import {addIngredients} from "Redux/slices/ingredientSlice"

export const useHydrateStore = (user: User | null) => {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const dispatch = useDispatch()

  useEffect(() => {
    const loadIngredients = async () => {
      try {
        setLoading(true)

        if (!user) {
          setLoading(false)
          setError("No user found")
          return
        }

        return await getIngredientsByUser(user.uid)
      } catch {
        setLoading(false)
      }
    }

    loadIngredients().then((result) => {
      if (!result) {
        setLoading(false)
        return
      }
      dispatch(addIngredients(result))
      setLoading(false)
    })

    return () => setError(null)
  }, [])

  return {loading, error}
}