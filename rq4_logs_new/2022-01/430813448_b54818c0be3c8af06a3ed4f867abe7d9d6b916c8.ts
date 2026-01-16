import { ExternalMovies } from "backend/ApiTypesObjects/ApiTypeExternalMovies";
import { httpController } from "backend/controllers/htppController";
import { ApiExternalResponse } from "backend/types/ApiExternalResponse";

export class getSearchService {
    execute(page: number, search: string) {
        const http = new httpController();
        
        return http
        .handle( new ExternalMovies() )
        .get<ApiExternalResponse>(`/search/multi?api_key=${process.env.REACT_APP_BASE_API_FILMES_KEY}&page=${page}&query=${search}&language=pt-BR`);
    }
}