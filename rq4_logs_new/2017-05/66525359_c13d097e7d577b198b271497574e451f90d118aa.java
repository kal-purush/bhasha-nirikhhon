/*  Copyright 2015 Sergey Vostokin, Ekaterina Skoryupina                    */
/*                                                                          */
/*  Licensed under the Apache License, Version 2.0 (the "License");         */
/*  you may not use this file except in compliance with the License.        */
/*  You may obtain a copy of the License at                                 */
/*                                                                          */
/*  http://www.apache.org/licenses/LICENSE-2.0                              */
/*                                                                          */
/*  Unless required by applicable law or agreed to in writing, software     */
/*  distributed under the License is distributed on an "AS IS" BASIS,       */
/*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.*/
/*  See the License for the specific language governing permissions and     */
/*  limitations under the License.                                          */
/*--------------------------------------------------------------------------*/
package engine_msg_queue;

import java.util.LinkedList;
import java.util.Queue;

public abstract class Actor extends Thread {
    Engine engine;
    final Queue<Message> queue = new LinkedList<>();

    abstract public void recv(Message message);

    public void startActor() {
        this.start();
    }

    public void run() {
        Message msg;

        for (; ; ) {
            synchronized (this) {
                while (queue.isEmpty()) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                msg = queue.poll();
            }
            msg.setSending(false);
            recv(msg);
        }
    }

    public void finished() {
        synchronized (engine) {
            engine.setFinished(true);
            engine.notify();
        }
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public Queue<Message> getQueue() {
        return queue;
    }
}