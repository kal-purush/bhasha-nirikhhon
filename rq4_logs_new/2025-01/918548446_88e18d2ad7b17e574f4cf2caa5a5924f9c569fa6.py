import gymnasium as gym
import ale_py
import websockets
import asyncio
import json
import numpy as np
import cv2

async def send_frames(websocket):
    env = gym.make('ALE/Pong-v5', render_mode='rgb_array')
    obs, info = env.reset()
    try:
        global Takeaction
        Takeaction = 0
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=1/30)
                action = json.loads(message)
                
                if action['moveDown'] == 1:
                    Takeaction = 3
                elif action['moveUP'] == 1:
                    Takeaction = 2
                elif action['moveUP'] == 0 and action['moveDown'] == 0:
                    Takeaction = 0
                    
            except asyncio.TimeoutError:
                pass
            
            obs, reward, terminated, truncated, info = env.step(Takeaction)
            _, buffer = cv2.imencode('.jpg', obs)
            frame = buffer.tobytes()
            await websocket.send(frame)
            await asyncio.sleep(1/30)  # 30 FPS
            if terminated or truncated:
                obs, info = env.reset()
    finally:
        env.close()

async def main():
    async with websockets.serve(send_frames, "0.0.0.0", 8765):
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())