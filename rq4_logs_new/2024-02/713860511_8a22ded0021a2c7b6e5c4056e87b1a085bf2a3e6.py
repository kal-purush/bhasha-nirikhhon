import asyncio

async def coro1():
    print("Coro1 começou")
    await asyncio.sleep(2)
    print("Coro1 terminou")

async def coro2():
    print("Coro2 começou")
    try:
        await asyncio.sleep(3)
    except asyncio.CancelledError:
        print("Coro2 foi cancelado")
        raise
    print("Coro2 terminou")

async def main():
    task1 = asyncio.create_task(coro1())
    task2 = asyncio.create_task(coro2())

    # Espera até que a primeira tarefa seja concluída
    await task1
    # Cancela a execução da segunda tarefa
    task2.cancel()

asyncio.run(main())