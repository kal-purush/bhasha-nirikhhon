import Head from 'next/head'
import styles from '../styles/Home.module.css'
function Deployments() {
    return (
        <div className={styles.container}>
            <Head>
                <title>Deployments</title>
                <meta name="description" content="A deployments page" />
                <link rel="icon" href="/favicon.ico" />
            </Head>

            <main className={styles.main}>
                <h1 className={styles.title}>
                    Deployments!
                </h1>
                <div>Deployments</div>
            </main>
        </div>
    )
}

export default Deployments