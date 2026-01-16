import Link from 'next/link'

const Notfound = () => {
    return ( 
        <div className='not__found'>
            <h1>ooooops....</h1>
            <h2>This page can not be found</h2>
            <p>return to <Link href='/'>Homepage</Link></p>
        </div>
     );
}
 
export default Notfound;