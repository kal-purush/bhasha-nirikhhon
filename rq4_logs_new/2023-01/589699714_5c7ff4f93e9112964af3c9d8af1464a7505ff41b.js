import spinner from './spinner.gif'

export const Loader = () => {
  return (
    <div className='loader'>
        <img src={spinner} alt='loading'/>
        <h1>fetching Data ...</h1>
    </div>
  )
}