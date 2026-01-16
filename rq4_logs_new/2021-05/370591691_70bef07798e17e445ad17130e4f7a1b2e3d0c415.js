import { useState, useEffect } from 'react';
import useSWR from 'swr';
import axios from 'axios';

import styles from '../styles/Posts.module.css'

const fetcher = (url) => axios.get(url).then((res) => res.data);

const useMounted = () => {
    const [mounted, setMounted] = useState(false)
    useEffect(() => setMounted(true), [])
    return mounted
}

function Posts() {
    const mounted = useMounted();
    const { data, error } = useSWR(() => (mounted ? '/api/posts' : null), fetcher)

    if (error) {
        return (<div>Error</div>);
    }
    if (!data) {
        return (<div>Load</div>);
    }

    if (data) {
        return (
            <div className={styles.postsContainer}>
                {data.posts.map((post, i) => (
                    <div key={i} className={styles.postsElem}>
                        {post.title}
                        {post.content}
                    </div>
                ))}
            </div>
        )
    } else {
        return (<div>Error in load</div>)
    }
}

export default Posts;