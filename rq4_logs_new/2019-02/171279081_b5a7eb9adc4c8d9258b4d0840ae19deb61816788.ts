import { Model } from 'mongoose';
import { CreateNewsDto } from './dto/CreateNewsDto';
import { Injectable } from "@nestjs/common";
import { InjectModel } from "@nestjs/mongoose";
import { News } from "./interfaces/news.interface";

@Injectable()
export class NewsService {
    constructor(@InjectModel('News') private readonly newsModel: Model<News>) {}

    async create(createNewsDto: CreateNewsDto): Promise<News> {
        const createdNews = new this.newsModel(createNewsDto);
        return await createdNews.save();
    }

    async findAll(): Promise<News> {
        return await this.newsModel.find().exec();
    }
}