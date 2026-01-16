/**
 * Unit test for QueueService
 */

import { async } from '@angular/core/testing';
import { QueueService } from './queue.service';

describe('Queue Service', () => {
  /*
   * Queue Management
   */

  it(`should correctly add item to queue`, async(() => {
    let queue: QueueService<any> = createQueueService();
    let item: string = "item";

    queue.add(item);

    expect(queue.list().length).toEqual(1);
    expect(queue.list()).toContain(item);
  }));

  it(`should correctly remove item from queue`, async(() => {
    let queue: QueueService<any> = createQueueService();
    let item: string = "item";

    queue.add(item);
    queue.remove(0);

    expect(queue.list().length).toEqual(0);
    expect(queue.list()).not.toContain(item);
  }));

  it(`should correctly clear queue`, async(() => {
    let queue: QueueService<any> = createQueueService();
    let item: string = "item";

    queue.add(item);
    queue.clear();

    expect(queue.list().length).toEqual(0);
    expect(queue.list()).not.toContain(item);
  }));

  /**
   * Playback
   */

  it(`should not be playing when newly initialized`, async(() => {
    let queue: QueueService<any> = createQueueService();

    expect(queue.isPlaying()).toEqual(false);
  }));

  it(`should not play when queue is empty`, async(() => {
    let queue: QueueService<any> = createQueueService();

    queue.play();
    
    expect(queue.isPlaying()).toEqual(false);
  }));

  it(`should start playing automatically when an item is added to queue`, async(() => {
    let queue: QueueService<any> = createQueueService();
    let item: string = "item";

    queue.add(item);

    expect(queue.isPlaying()).toEqual(true);
  }));

  it(`should stop playing on stop`, async(() => {
    let queue: QueueService<any> = createQueueService();
    let item: string = "item";

    queue.add(item);
    expect(queue.isPlaying()).toEqual(true);

    queue.stop();

    expect(queue.isPlaying()).toEqual(false);
  }));


  it(`should start playing on play`, async(() => {
    let queue: QueueService<any> = createQueueService();
    let item: string = "item";

    queue.add(item);
    queue.stop();
    queue.play();

    expect(queue.isPlaying()).toEqual(true);
  }));

  it(`should update state on next`, async(() => {
    let queue: QueueService<any> = createQueueService();
    let item1: string = "item 1";
    let item2: string = "item 2";

    queue.add(item1);
    queue.add(item2);

    expect(queue.isPlaying()).toEqual(true);
  }));


});

class Player<I> {
  isPlaying: boolean = false;
  play(item: I): void { this.isPlaying = true; }
  stop(): void { this.isPlaying = false; }
}

function createQueueService(): QueueService<any> {
  let player = new Player();
  let queue: QueueService<any> = new QueueService<any>(
    (item) => player.play(item),
    () => player.stop(),
    () => player.isPlaying
  );

  return queue;
}