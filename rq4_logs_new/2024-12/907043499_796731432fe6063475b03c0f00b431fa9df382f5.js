async function init() {
    if (!navigator.gpu) {
        console.error("WebGPU is not supported!");
        return;
    }

    const adapter = await navigator.gpu.requestAdapter();
    const device = await adapter.requestDevice();

    const canvas = document.getElementById('canvas');
    const context = canvas.getContext('webgpu');

    const swapChainFormat = "bgra8unorm";
    context.configure({
        device: device,
        format: swapChainFormat
    });

    const vertexModule = device.createShaderModule({
        code: `...vertex shader code...`
    });

    const fragmentModule = device.createShaderModule({
        code: `...fragment shader code...`
    });

    const pipeline = device.createRenderPipeline({
        vertex: {
            module: vertexModule,
            entryPoint: "main"
        },
        fragment: {
            module: fragmentModule,
            entryPoint: "main",
            targets: [{
                format: swapChainFormat
            }]
        },
        primitive: {
            topology: "triangle-list"
        }
    });

    function frame() {
        const commandEncoder = device.createCommandEncoder();
        const textureView = context.getCurrentTexture().createView();
        const renderPassDescriptor = {
            colorAttachments: [{
                view: textureView,
                loadValue: {r: 0, g: 0, b: 0, a: 1},
                storeOp: "store"
            }]
        };

        const passEncoder = commandEncoder.beginRenderPass(renderPassDescriptor);
        passEncoder.setPipeline(pipeline);
        passEncoder.draw(6, 1, 0, 0);
        passEncoder.endPass();

        device.queue.submit([commandEncoder.finish()]);

        requestAnimationFrame(frame);
    }

    requestAnimationFrame(frame);
}

window.onload = init;