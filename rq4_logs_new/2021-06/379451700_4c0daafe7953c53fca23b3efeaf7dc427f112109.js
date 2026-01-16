import { useRouter } from 'next/router'
import styles from '../../styles/Home.module.css'

export default function New() {
  const router = useRouter();
  return (
    <div className={styles.container}>
        Query: {router.query.param}
    </div>
  )
}