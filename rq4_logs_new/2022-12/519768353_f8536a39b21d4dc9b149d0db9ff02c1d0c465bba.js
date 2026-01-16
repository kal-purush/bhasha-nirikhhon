class ScratchCam {
    getInfo() {
        id: 'scratchcam'
        name: 'Scratch Cam'
        blocks: [
            {
                opcode: 'takephoto',
                blockType: ScratchCam.BlockType.REPORTER,
                text: 'Take a Photo and get File'
            }
        ]
    }
    takephoto() {
        var canvas = document.getElementsByTagName("canvas")[0];
        var width = 480;
        var height = 360;
        var context = canvas.getContext('2d');
        canvas.width = width;
        canvas.height = height;
        context.drawImage(video, 0, 0, width, height);
        var data = canvas.toDataURL('image/png');
        return data
    }
}

Scratch.extensions.register(new ScratchCam())