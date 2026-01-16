import React from "react";
import axios from "axios";
import FormInput from "./FormInput";
import FormTextArea from "./FormTextArea";

class EditAlbum extends React.Component {
    state = {
        id:this.props.album.id,
        title:this.props.album.title,
        artist:this.props.album.artist,
        description:this.props.album.description,
        year:this.props.album.year,
        image:this.props.album.image_name,
        tracks:this.props.album.tracks,

    }

    updateTitle = (t) => {
        this.setState({title: t})
        console.log("State of form: ", this.state)
    }
    updateArtist = (t) => {
        this.setState({artist: t})
        console.log("State of form: ", this.state)
    }
    updateYear = (t) => {
        this.setState({year: t})
        console.log("State of form: ", this.state)
    }
    updateDescription = (t) => {
        this.setState({description: t})
        console.log("State of form: ", this.state)
    }
    updateImage = (t) => {
        this.setState({image: t})
        console.log("State of form: ", this.state)
    }

    saveAlbum = async (album) => {
        axios.put('http://localhost:3100/albums', album)
            .then(results => {
                console.log(results)
                console.log(results.data)
            })
    }

    handleFormSubmit = (event) => {
        event.preventDefault()
        console.log("Form Submit = ", this.state)
        this.saveAlbum(this.state)
    }

    render() {
        return (
            <div className="container">
                <form onSubmit={this.handleFormSubmit}>
                    <div className="form-group">
                        <h1>Edit Album</h1>
                        <FormInput id="albumTitle" title="Album Title" value={this.props.album.title} onChange={this.updateTitle}/>
                        <FormInput id="albumArtist" title="Album Artist" value={this.props.album.artist} onChange={this.updateArtist} />
                        <FormTextArea id="albumDescription" title="Album Description" value={this.props.album.description} onChange={this.updateDescription} />
                        <FormInput id="albumYear" title="Year" value={this.props.album.year} onChange={this.updateYear}/>
                        <FormInput id="imageUrl" title="Image" value={this.props.album.image_name} onChange={this.updateImage}/>
                    </div>
                    <button type="submit" className="btn btn-primary">Submit</button>
                    <button type="reset" className="btn btn-light">Cancel</button>
                </form>
            </div>
        )
    }
}
export default EditAlbum