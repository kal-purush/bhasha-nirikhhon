import React, { useState, useEffect } from "react";
import { View, Text, SafeAreaView, Modal, TouchableOpacity, Alert, ScrollView, TextInput, KeyboardAvoidingView, FlatList, Image } from "react-native";
import styles from "./ListDetailsStyles";

import MovSerCard from "../../../components/Card/MoviesCard/MoviesCard";
import Input from "../../../components/Input/Input";

import { FAB } from "react-native-paper";
import Icon from "react-native-vector-icons/Ionicons";

import AsyncStorage from '@react-native-async-storage/async-storage';

import axios from "react-native-axios";

const API_KEY = '6d0b2bd6b37b82532732bc7f0db0df55';
const BASE_URL = 'https://api.themoviedb.org/3';
const IMAGE_BASE_URL = 'https://image.tmdb.org/t/p/w200';

function ListDetails({ navigation, route }) {

    const [modalVisible, setModalVisible] = useState(false);
    const [savedMovies, setSavedMovies] = useState([]);
    const [searchMovie, setSearchMovie] = useState('');
    const [searchText, setSearchText] = useState('');
    const [searchResults, setSearchResults] = useState([]);
    const [selectedMovie, setSelectedMovie] = useState(null);
    const [genreNames, setGenreNames] = useState([]);
    const [categoryText, setCategoryText] = useState("");
    const [duration, setDuration] = useState("");
    
    useEffect(() => {
        // Kaydedilmiş filmleri AsyncStorage'den al
        fetchSavedMovies();
        //clearData();
        if (route.params && route.params.Movie) {
            const { Movie } = route.params;
            // Eğer bir film aktarıldıysa, savedMovies dizisine ekleyin
            const updatedMovies = [Movie, ...savedMovies];
            setSavedMovies(updatedMovies);
            AsyncStorage.setItem("savedMovies", JSON.stringify(updatedMovies))
              .then(() => {
                console.log("Film başarıyla eklendi.");
                fetchSavedMovies();
              })
              .catch((error) => {
                console.log("Film eklenirken bir hata oluştu:", error);
              });
      
            // route.params'ı temizleyin, böylece tekrar açıldığında Movie verisi yok olur
            navigation.setParams({ Movie: null });
          }
        }, [route.params]);

    const fetchSavedMovies = async () => {
        try {
            const movies = await AsyncStorage.getItem('listDetails');
            if (movies) {
                setSavedMovies(JSON.parse(movies));
            }
        } catch (error) {
            console.log('Hata: ', error);
        }
    };

    const handleFabPress = () => {
        setModalVisible(true);
    };

    const closeModal = () => {
        setModalVisible(false);
    };

    const saveMovie = async () => {
        // Verileri bir obje olarak hazırla
        const movieData = {
            movieId: selectedMovie.id,
            movieName: selectedMovie.title,
            movieDate: formatDate(selectedMovie.release_date),
            movieVote: selectedMovie.vote_average.toFixed(1),
            movieCategory: categoryText,
            moviePoster: selectedMovie.poster_path,
            movieTime: duration
        };

        try {
            // Daha önce kaydedilen filmleri al
            const existingMovies = await AsyncStorage.getItem('listDetails');
            let updatedListDetails = [];

            if (existingMovies) {
                // Eğer daha önce kaydedilen filmler varsa, onları güncelle
                updatedListDetails = JSON.parse(existingMovies);
            }

            // Yeni filmi ekle
            updatedListDetails.unshift(movieData);

            // Filmleri AsyncStorage'e kaydet
            await AsyncStorage.setItem('listDetails', JSON.stringify(updatedListDetails));

            // Kaydedilen filmleri güncelle
            setSavedMovies(updatedListDetails);

            // Modalı kapat
            setSearchResults("");
            setSelectedMovie("");
            setSearchText("");
            closeModal();
        } catch (error) {
            console.log('Hata: ', error);
        }
    };

    const searchMovies = async () => {
        try {
            const response = await axios.get(`${BASE_URL}/search/movie`, {
                params: {
                    api_key: API_KEY,
                    query: searchText,
                },
            });

            const results = response.data.results.slice(0, 4);
            setSearchResults(results);
        } catch (error) {
            console.error(error);
        }
    };

    const getMovieDetails = async (movieId) => {
        try {
            const response = await axios.get(`${BASE_URL}/movie/${movieId}`, {
                params: {
                    api_key: API_KEY,
                },
            });
            const runtime = response.data.runtime;
            const formattedDuration = formatDuration(runtime);
            setDuration(formattedDuration);

        } catch (error) {
            console.error(error);
        }
    };

    const formatDuration = (minutes) => {
        const hours = Math.floor(minutes / 60);
        const remainingMinutes = minutes % 60;

        return `${hours} h ${remainingMinutes} m`;
    };

    const formatDate = (dateString) => {
        const date = new Date(dateString);
        const formattedDate = date.toLocaleDateString('tr-TR'); // tr-TR, Türkiye'nin bölgesel kodudur

        return formattedDate;
    };


    const fetchGenreNames = async (genreIds) => {
        try {
            const response = await axios.get(`${BASE_URL}/genre/movie/list`, {
                params: {
                    api_key: API_KEY,
                },
            });

            const genres = response.data.genres;
            const names = genreIds.map((genreId) => {
                const genre = genres.find((g) => g.id === genreId);
                return genre ? genre.name : '';
            });

            return names;
        } catch (error) {
            console.error(error);
            return [];
        }
    };

    const handleTextChange = (text) => {
        setSearchText(text);
        searchMovies();
    };

    const handleMovieSelect = async (movie) => {
        //console.log(movie);
        setSelectedMovie(movie);
        setSearchText(movie.title);
        getMovieDetails(movie.id);
        // Film tür adlarını al
        const genreNames = await fetchGenreNames(movie.genre_ids);

        // Kategori adlarını ekrana yazdır
        setCategoryText(genreNames.length > 0 ? genreNames.join(', ') : 'Belirtilmemiş');
    };

    const handleSearchBarPress = () => {
        setSelectedMovie(null);
        setGenreNames([]);
    };

    const handleMovieDelete = (movie) => {
        Alert.alert(
            'Film Silme',
            `"${movie.movieName}" Filmini silmek istediğinize emin misiniz?`,
            [
                {
                    text: 'Vazgeç',
                    style: 'cancel',
                },
                {
                    text: 'Sil',
                    style: 'destructive',
                    onPress: () => deleteMovie(movie),
                },
            ],
            { cancelable: false }
        );
    };

    const deleteMovie = async (movie) => {
        const updatedListDetails = savedMovies.filter((m) => m.movieId !== movie.movieId);
        setSavedMovies(updatedListDetails);
        AsyncStorage.setItem('listDetails', JSON.stringify(updatedListDetails))
            .then(() => {
                console.log('Film başarıyla silindi.');
            })
            .catch((error) => {
                console.log('Film silinirken bir hata oluştu:', error);
            });
    };

    const renderMovieItem = ({ item }) => (
        <TouchableOpacity onPress={() => handleMovieSelect(item)}>
            <View style={{ flexDirection: 'row', alignItems: 'center' }}>
                <Image
                    source={{ uri: `${IMAGE_BASE_URL}${item.poster_path}` }}
                    style={{ width: 50, height: 75, margin: 10 }}
                />
                <View>
                    <Text>{item.title} </Text>

                </View>

            </View>
        </TouchableOpacity>
    );

    function back() {
        navigation.goBack();
    };

    return (
        <SafeAreaView style={styles.container}>
            <View style={styles.customHeader}>
                <Icon name="arrow-back" size={22} color={"black"} style={styles.backIcon} onPress={back} />
                <Text style={styles.headerText} >Deneme</Text>
            </View>
            <KeyboardAvoidingView style={styles.container} >
                <View style={{ flexDirection: "row", backgroundColor: "white", opacity: 0.7 }} >
                    <View style={styles.search} >
                        <Icon name="search" size={20} color={"black"} style={styles.icon} />
                        <TextInput placeholder="Search Movie Name" placeholderTextColor={"black"} value={searchMovie}
                            onChangeText={setSearchMovie} />
                    </View>
                </View>
                <View style={styles.seperator} />
                <ScrollView>
                    <View style={styles.content}>
                        {savedMovies
                            .filter(
                                (movie) =>
                                    movie.movieName.toLowerCase().includes(searchMovie.toLowerCase())
                            )
                            .map((movie, index) => (
                                <MovSerCard
                                    key={movie.movieId}
                                    movieName={movie.movieName}
                                    date={movie.movieDate}
                                    vote={movie.movieVote}
                                    category={movie.movieCategory}
                                    poster={movie.moviePoster}
                                    time={movie.movieTime}
                                    onPressList={null}
                                    onPressDelete={() => handleMovieDelete(movie)}
                                    iconName={"library-add"}
                                />
                            ))}
                    </View>
                </ScrollView>
                <FAB
                    style={styles.fab}
                    icon="plus"
                    label="Ekle"
                    color="white"
                    onPress={handleFabPress}
                />

            </KeyboardAvoidingView>

            <Modal
                visible={modalVisible}
                transparent={true}
                animationType="fade"
                onRequestClose={closeModal}
            >
                <TouchableOpacity
                    style={styles.modalBackground}
                    activeOpacity={1}
                    onPress={closeModal}
                >
                    <View style={styles.modalContainer}>
                        <TouchableOpacity
                            activeOpacity={1}
                            style={styles.modalContent}
                            onPress={() => { }}
                        >
                            <View>
                                <View style={styles.searchMovie} >
                                    <Icon name={"search"} size={16} color="black" style={styles.icon} />
                                    <TextInput
                                        value={searchText}
                                        onChangeText={handleTextChange}
                                        placeholder="Film İsmi Ara..."
                                        onFocus={handleSearchBarPress}
                                        style={styles.searchText}
                                    />
                                </View>

                                {selectedMovie ? (
                                    <View>
                                        <View style={styles.seperator2} />
                                        <Input label={"Seçilen Film"} text={selectedMovie.title} />
                                        <View style={{ flexDirection: "row" }} >
                                            <View style={{ flex: 1, marginRight: 10, }} >
                                                <Input label={"Çıkış Tarihi"} text={formatDate(selectedMovie.release_date)} />
                                            </View>
                                            <View style={{ flex: 1 }}>
                                                <Input label={"Puanı"} text={selectedMovie.vote_average.toFixed(1)} />
                                            </View>
                                        </View>
                                        <Input label={"Kategorileri"} text={categoryText} />

                                        <TouchableOpacity style={styles.button} onPress={saveMovie} >
                                            <Text style={styles.buttonText} >Filmi Kaydet</Text>
                                        </TouchableOpacity>
                                    </View>
                                ) : (
                                    <FlatList
                                        data={searchResults}
                                        keyExtractor={(item) => item.id.toString()}
                                        renderItem={renderMovieItem}
                                    />
                                )}

                            </View>



                        </TouchableOpacity>
                    </View>
                </TouchableOpacity>
            </Modal>

        </SafeAreaView>


    )
};

export default ListDetails;
