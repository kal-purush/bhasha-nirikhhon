import { initializeApp, cert } from 'firebase-admin/app'
import { getFirestore } from 'firebase-admin/firestore'

const SERVICE_ACCOUNT_KEY = process.env.SERVICE_ACCOUNT_KEY

const serviceAccount = SERVICE_ACCOUNT_KEY
  ? JSON.parse(SERVICE_ACCOUNT_KEY)
  : null

initializeApp({
  credential: cert(serviceAccount)
})

const db = getFirestore()

export default db