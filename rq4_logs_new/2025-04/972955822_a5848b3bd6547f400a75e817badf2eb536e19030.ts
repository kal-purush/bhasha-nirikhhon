import {
  type Firestore,
  QueryConstraint,
  Timestamp,
  addDoc,
  collection,
  doc,
  getDocs,
  limit,
  orderBy,
  query,
  where,
} from 'firebase/firestore';
import { v4 as uuidv4 } from 'uuid';
import { z } from 'zod';
import type { EventCollection, EventDocument, EventType, FirestoreDocument } from './types';
import { serverTimestamp } from './utils';

// Base event types for CRUD operations
export type CreateEvent<T> = EventDocument<T, 'create'>;
export type UpdateEvent<T> = EventDocument<T & { changes: Partial<T> }, 'update'>;
export type DeleteEvent<T> = EventDocument<{ id: string }, 'delete'>;

// Helper to create an event document
export async function createEvent<T, E extends EventType>(
  db: Firestore,
  eventCollection: EventCollection<T, E>,
  entityId: string,
  type: E,
  data: T,
  metadata?: Record<string, unknown>
): Promise<string> {
  const eventDoc = {
    entityId,
    type,
    data,
    clientTimestamp: new Date(),
    serverTimestamp: serverTimestamp(),
    metadata: metadata || {},
  };

  const docRef = await addDoc(eventCollection.ref(db), eventDoc);
  return docRef.id;
}

// Helper to get all events for an entity
export async function getEntityEvents<T, E extends EventType>(
  db: Firestore,
  eventCollection: EventCollection<T, E>,
  entityId: string
): Promise<EventDocument<T, E>[]> {
  const q = query(
    eventCollection.ref(db),
    where('entityId', '==', entityId),
    orderBy('clientTimestamp')
  );

  const querySnapshot = await getDocs(q);
  return querySnapshot.docs.map((doc) => doc.data());
}

// Helper to get the latest entity state by reconstructing from events
export async function getLatestEntityState<T>(
  db: Firestore,
  eventCollection: EventCollection<T>,
  entityId: string
): Promise<FirestoreDocument<T> | null> {
  const events = await getEntityEvents(db, eventCollection, entityId);

  if (events.length === 0) {
    return null;
  }

  // Find the initial create event
  const createEvent = events.find((event) => event.type === 'create');
  if (!createEvent) {
    return null;
  }

  // Start with the create event data
  let currentState: Record<string, unknown> = {
    ...createEvent.data,
    id: entityId,
    clientTimestamp: createEvent.clientTimestamp,
    serverTimestamp: createEvent.serverTimestamp,
  };

  // Apply all update events in order
  const updateEvents = events
    .filter((event) => event.type === 'update')
    .sort((a, b) => a.clientTimestamp.getTime() - b.clientTimestamp.getTime());

  for (const event of updateEvents) {
    const updateEvent = event as UpdateEvent<T>;
    currentState = {
      ...currentState,
      ...updateEvent.data.changes,
      clientTimestamp: updateEvent.clientTimestamp,
      serverTimestamp: updateEvent.serverTimestamp,
    };
  }

  // Check if entity was deleted
  const wasDeleted = events.some((event) => event.type === 'delete');
  if (wasDeleted) {
    return null;
  }

  return currentState as FirestoreDocument<T>;
}

// Create a new entity
export async function createEntity<T>(
  db: Firestore,
  eventCollection: EventCollection<T, 'create'>,
  data: T,
  entityId: string = uuidv4()
): Promise<string> {
  await createEvent(db, eventCollection, entityId, 'create', data);
  return entityId;
}

// Update an existing entity
export async function updateEntity<T>(
  db: Firestore,
  eventCollection: EventCollection<T & { changes: Partial<T> }, 'update'>,
  entityId: string,
  changes: Partial<T>,
  currentData: T
): Promise<void> {
  await createEvent(db, eventCollection, entityId, 'update', { ...currentData, changes });
}

// Delete an entity
export async function deleteEntity<T>(
  db: Firestore,
  eventCollection: EventCollection<{ id: string }, 'delete'>,
  entityId: string
): Promise<void> {
  await createEvent(db, eventCollection, entityId, 'delete', { id: entityId });
}