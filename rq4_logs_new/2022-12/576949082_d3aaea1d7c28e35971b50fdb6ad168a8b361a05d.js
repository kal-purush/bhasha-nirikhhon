import './albums.scss';
import { useSelector } from 'react-redux';

function AlbumComponent(props) {
    return (

        <div className="Album">
            <img className='AlbumImage' src={props.Album.cover_small
            } alt="Album" />
            <div className='AlbumInfoText'>
                <h3>{props.Album.title}</h3>

            </div>

        </div>
    )
}

function Albums() {

    let Albums = useSelector(state => state.search.albums);
    console.log(Albums)

    return (
        <div className="list" >
            <h1>Albums</h1>

            {
                Albums.map((Album, index) => {
                    return <AlbumComponent Album={Album} key={index} />
                }
                )
            }
        </div>

    );
}

export default Albums;