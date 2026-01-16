import { Request, Response, NextFunction } from 'express';
import CarouselSlide from '../models/CarouselSlide';
import errorHandler from '../utils/errorHandler';
import APIError from '../utils/APIError';
import { checkIfCarouselSlideExists,checkIfSlideOrderExists, checkIfSlideTitleExists,
} from '../services/carouselSlideService';

const createNewCarouselSlide = errorHandler(
  async(req: Request, res: Response, next: NextFunction): Promise<void> => {
    const { slideOrder, imageUrl, title, description } = req.body;

    // Check if the slideOrder already exists
    const slideOrderExists = await checkIfSlideOrderExists(slideOrder);
    if (slideOrderExists) {
      return next(new APIError('Slide order already exists.', 400));
    }

    // Check if the title already exists
    const titleExists = await checkIfSlideTitleExists(title);
    if (titleExists) {
      return next(new APIError('Title already exists.', 400));
    }

    // Create the new carousel slide
    const carouselSlide = await CarouselSlide.create({ slideOrder, imageUrl, title, description });

    res.status(201).json({ status: 'success', carouselSlide });
  });

const getAllCarouselSlides = errorHandler(
  async(req: Request, res: Response, next: NextFunction) => {
    const carouselSlides = await CarouselSlide.findAll();
    res.status(200).json({
      status: 'success',
      carouselSlides: carouselSlides.length > 0 ? carouselSlides : 'No carousel slides found.',
    });
  },
);

const getCarouselSlideById = errorHandler(
  async(req: Request, res: Response, next: NextFunction) => {
    const { id } = req.params;
    const carouselSlide = await checkIfCarouselSlideExists({ id });

    if (!carouselSlide) {
      return next(new APIError('Carousel Slide not found.', 404));
    }

    res.status(200).json({ status: 'success', carouselSlide });
  },
);

const updateCarouselSlideById = errorHandler(
  async(req: Request, res: Response, next: NextFunction) => {
    const { id } = req.params;
    const { slideOrder, imageUrl, title, description } = req.body;

    const carouselSlide = await checkIfCarouselSlideExists({ id });
    if (!carouselSlide) {
      return next(new APIError('Carousel Slide not found.', 404));
    }

    // Check if the slideOrder already exists
    const slideOrderExists = await checkIfSlideOrderExists(slideOrder);
    if (slideOrderExists) {
      return next(new APIError('Slide order already exists.', 400));
    }

    // Check if the title already exists
    const titleExists = await checkIfSlideTitleExists(title);
    if (titleExists) {
      return next(new APIError('Title already exists.', 400));
    }

    // Update carousel slide with only the provided fields
    const updatedFields = {
      ...(slideOrder !== undefined && { slideOrder }),
      ...(imageUrl && { imageUrl }),
      ...(title && { title }),
      ...(description && { description }) };
    await carouselSlide.update(updatedFields);

    res.status(200).json({ status: 'success', carouselSlide });
  },
);

const deleteCarouselSlideById = errorHandler(
  async(req: Request, res: Response, next: NextFunction) => {
    const { id } = req.params;
    const carouselSlide = await checkIfCarouselSlideExists({ id });

    if (!carouselSlide) {
      return next(new APIError('Carousel Slide not found.', 404));
    }

    await carouselSlide.destroy();
    res.status(202).json({ status: 'success' });
  },
);

export {
  createNewCarouselSlide,
  getAllCarouselSlides,
  getCarouselSlideById,
  updateCarouselSlideById,
  deleteCarouselSlideById,
};