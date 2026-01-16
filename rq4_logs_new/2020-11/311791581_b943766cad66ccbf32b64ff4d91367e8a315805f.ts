import axios from 'axios';
import { IImportMovieResponseDTO } from 'src/useCases/ImportMovie/ImportMovieDTO';

import { IMoviesService } from '../IMoviesService';
import { IImportMovieResponseDTO } from '../useCases/ImportMovie/ImportMovieDTO';

export class TheMovieDBService implements IMoviesService {
  async getMovie(id: number): Promise<IImportMovieResponseDTO> {
    console.log('Puxando filmes da API');

    try {
      const { data } = await axios.get(`https://api.themoviedb.org/3/movie/${id}?api_key=957d44cc233e5e3e4d291376c005abb7`);

      const importMovieDTO:IImportMovieResponseDTO = {
        id: data.id,
        title: data.title,
        overview: data.overview,
      };
      return (importMovieDTO);
    } catch (error) {
      throw new Error(error);
    }
  }

  getTranslations(id: number): Promise<any> {
    throw new Error('Method not implemented.');
  }
}