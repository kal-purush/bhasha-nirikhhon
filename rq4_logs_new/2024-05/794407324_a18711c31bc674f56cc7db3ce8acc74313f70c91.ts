// Importando topos e funções necessárias
import { Book } from "../models/Book.js";
import { BookService } from "../services/BookService.js";
import type { Request, Response } from "express";

/**
 * Definindo a interface para o controller de livros
 * @interface
 */
export interface IBookController {
    save(request: Request, response: Response): Promise<Response>;
    findAll(request: Request, response: Response): Promise<Response>;
    findById(request: Request, response: Response): Promise<Response>;
    delete(request: Request, response: Response): Promise<Response>;
}

// Instanciando o serviço de livros
const bookService = new BookService()

/**
 * Controla gerenciamento de livros
 * @class
 */
export class BookController implements IBookController {
    /**
     * Salva um novo livro no sistema.
     * @param request - A requisição HTTP contendo os dados do livro.
     * @param response - A resposta HTTP a ser enviada ao cliente.
     * @returns Uma promessa que resolve para a resposta HTTP.
     */
    async save(request: Request, response: Response): Promise<Response> {
        try {
            // Extrai os dados do corpo da requisição
            const { name, author, year, price } = request.body;
    
            // Verifica se todos os campos obrigatórios estão presentes
            if (!name ||!author ||!year ||!price) {
                return response.status(400).json({ message: "Missing required fields" });
            }

            // Verifica se o livro já existe
            const hasItem = await bookService.checkIfBookExists(name, author)
            if (hasItem) {
                return response.status(400).json({
                    message: "Já existe um livro desse autor com esse título!"
                })
            }
    
            // Cria um novo livro
            const book = new Book({
                name: name,
                author: author,
                year: year,
                price: price,
            });
    
            // Manda o livro para o Service
            await bookService.save(book);
    
            // Retorna uma resposta de sucesso
            return response.status(201).json({ message: "Livro salvo com sucesso!", book });
        } catch (error) {
            console.error(error);
            // Retorna uma resposta de erro
            return response.status(500).json({ message: "Um erro ocorreu ao salvar livro" });
        }
    }
    /**
     * Busca todos os livros disponíveis.
     * @param request - A requisição HTTP.
     * @param response - A resposta HTTP a ser enviada ao cliente.
     * @returns Uma promessa que resolve para a resposta HTTP contendo todos os livros.
     */
    async findAll(request: Request, response: Response): Promise<Response> {
        try {
            // Busca todos os livros
            const books = await bookService.findAll()
             // Retorna uma resposta com os livros encontrados
            return response.status(200).json({ books })
        } catch (error) {
            // Retorna uma resposta de erro
            return response.status(500).json({ message: "Um erro ocorreu ao buscar livros" });
        }
    }
    /**
     * Busca um livro pelo seu ID.
     * @param request - A requisição HTTP contendo o ID do livro.
     * @param response - A resposta HTTP a ser enviada ao cliente.
     * @returns Uma promessa que resolve para a resposta HTTP contendo o livro encontrado.
     */
    async findById(request: Request, response: Response): Promise<Response> {
        try {
            // Extrai o ID do corpo da requisição
            const { id } = request.body

            // Verifica se o ID foi fornecido
            if (!id) {
                return response.status(400).json()
            }
            // Busca o livro pelo ID
            const book = await bookService.findById(id)

            // Verifica se o livro foi encontrado
            if (!book) {
                return response.status(404).json({
                    message: "Livro não encontrado!"
                })
            }

            // Retorna uma resposta com o livro encontrado
            return response.status(200).json({ book })
            
        } catch (error) {
            // Retorna uma resposta de erro
            return response.status(500).json({ message: "Um erro ocorreu ao buscar livros" });
        }
    }
    /**
     * Deleta um livro pelo seu ID.
     * @param request - A requisição HTTP contendo o ID do livro.
     * @param response - A resposta HTTP a ser enviada ao cliente.
     * @returns Uma promessa que resolve para a resposta HTTP indicando se a operação foi bem-sucedida.
     */
    async delete(request: Request, response: Response): Promise<Response> {
        try {
            // Extrai o ID do corpo da requisição
            const { id } = request.body

            // Verifica se o ID foi fornecido
            if (!id) {
                return response.status(400).json()
            }
            // Tenta deletar o livro pelo ID
            const deleted = await bookService.delete(id)

            // Verifica se o livro foi deletado
            if (!deleted) {
                return response.status(404).json({
                    message: "Livro não encontrado!"
                })
            }

            // Retorna uma resposta de sucesso
            return response.status(200).json({ message: "Livro deletado com sucesso!" })
            
        } catch (error) {
            // Retorna uma resposta de erro
            return response.status(500).json({ message: "Um erro ocorreu ao deletar livro", error: error });
        }
    }
}