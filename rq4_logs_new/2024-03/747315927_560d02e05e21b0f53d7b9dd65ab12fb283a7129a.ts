import { prisma } from '../../prisma/client.js';
import type { SleepWindow, Prisma } from '@prisma/client';
import { ensureError } from '../utils/error.js';

const get = async (windowId: string): Promise<SleepWindow | null | Error> => {
  try {
    const result = await prisma.sleepWindow.findUnique({
      where: {
        id: windowId
      }
    });
    return result;
  } catch (err) {
    throw ensureError(err);
  }
};

const getByEventId = async (
  eventId: string
): Promise<SleepWindow[] | Error> => {
  try {
    const result = await prisma.sleepWindow.findMany({
      where: {
        eventId: eventId
      }
    });
    return result;
  } catch (err) {
    throw ensureError(err);
  }
};

const create = async (
  sleepWindowData: Prisma.SleepWindowCreateInput
): Promise<SleepWindow | Error> => {
  try {
    const newSleepWindow = await prisma.sleepWindow.create({
      data: sleepWindowData
    });
    return newSleepWindow;
  } catch (err) {
    throw ensureError(err);
  }
};

const update = async (
  windowId: string,
  sleepWindowData: Prisma.SleepWindowUpdateInput
): Promise<SleepWindow | Error> => {
  try {
    const updatedSleepWindow = await prisma.sleepWindow.update({
      where: {
        id: windowId
      },
      data: sleepWindowData
    });
    return updatedSleepWindow;
  } catch (err) {
    throw ensureError(err);
  }
};

const destroy = async (windowId: string): Promise<SleepWindow | Error> => {
  try {
    const result = await prisma.sleepWindow.delete({
      where: {
        id: windowId
      }
    });
    return result;
  } catch (err) {
    throw ensureError(err);
  }
};

export const service = {
  get,
  getByEventId,
  create,
  update,
  destroy
};