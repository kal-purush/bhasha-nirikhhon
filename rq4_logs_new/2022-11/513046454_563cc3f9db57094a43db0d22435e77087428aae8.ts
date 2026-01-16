import DataLoader from 'dataloader';
import { In } from 'typeorm';
import { Updoot } from '../entities/Updoot';

interface UpdootKey { postId: number; userId: number }

const createUpdootKey = (updoot: UpdootKey) => `${updoot.userId}|${updoot.postId}`;

export const createUpdootLoader = () => new DataLoader<UpdootKey, Updoot | null>(async (updootSearch) => {
  console.log(updootSearch, 'updootSearch')
  const postIds = updootSearch.map((item) => item.postId);
  const userIds = updootSearch.map((item) => item.userId);
  const updoots = await Updoot.findBy({ userId: In(userIds), postId: In(postIds) })
  
  const updootIdsToUpdoot: Record<string, Updoot> = {};

  updoots.forEach((updoot) => {
    updootIdsToUpdoot[createUpdootKey(updoot)] = updoot;
  })

  return updootSearch.map((key) => updootIdsToUpdoot[createUpdootKey(key)]);
});