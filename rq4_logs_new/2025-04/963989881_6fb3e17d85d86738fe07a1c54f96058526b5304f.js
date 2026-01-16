document.addEventListener('DOMContentLoaded', () => {
    // --- Get references (Add new margin inputs) ---
    const photoInputs = document.querySelectorAll('.photo-input');
    const templateInput = document.getElementById('template');
    const previewTemplate = document.getElementById('previewTemplate');
    const generateBtn = document.getElementById('generateBtn');
    const canvas = document.getElementById('canvas');
    const ctx = canvas.getContext('2d');
    const resultImage = document.getElementById('resultImage');
    const downloadLink = document.getElementById('downloadLink');
    const statusElem = document.getElementById('status');
    const scaleSliders = document.querySelectorAll('.scale-slider');
    const offsetXSliders = document.querySelectorAll('.offset-slider-x');
    const offsetYSliders = document.querySelectorAll('.offset-slider-y');
    const previewBgs = [
        document.getElementById('previewBg0'), document.getElementById('previewBg1'),
        document.getElementById('previewBg2'), document.getElementById('previewBg3'),
    ];
    // NEW: Margin Inputs
    const gridMarginTopInput = document.getElementById('gridMarginTop');
    const gridMarginBottomInput = document.getElementById('gridMarginBottom');
    const gridMarginLeftInput = document.getElementById('gridMarginLeft');
    const gridMarginRightInput = document.getElementById('gridMarginRight');

    // --- State Management (No major changes needed here) ---
    let photoFiles = [null, null, null, null];
    let templateFile = null;
    let photoImageObjects = [null, null, null, null];
    let templateImageObject = null;
    let imageTransforms = [
        { scale: 1, offsetX: 50, offsetY: 50, dataUrl: null }, { scale: 1, offsetX: 50, offsetY: 50, dataUrl: null },
        { scale: 1, offsetX: 50, offsetY: 50, dataUrl: null }, { scale: 1, offsetX: 50, offsetY: 50, dataUrl: null },
    ];

    // --- Helper Functions (loadImage, updatePreviewBackground, resetImageState - Keep as before) ---
    function loadImage(src) { // ... (same as before)
        return new Promise((resolve, reject) => {
            const img = new Image();
            img.onload = () => resolve(img);
            img.onerror = (err) => reject(new Error(`Failed to load image: ${src.substring(0, 50)}...`));
            img.src = src;
        });
    }
    function updatePreviewBackground(index) { // ... (same as before)
        const transform = imageTransforms[index];
        const previewBg = previewBgs[index];
        if (transform.dataUrl) {
            previewBg.style.backgroundImage = `url(${transform.dataUrl})`;
            previewBg.style.backgroundSize = `${transform.scale * 100}%`;
            previewBg.style.backgroundPosition = `${transform.offsetX}% ${transform.offsetY}%`;
        } else {
             previewBg.style.backgroundImage = '';
             previewBg.style.backgroundSize = 'cover';
             previewBg.style.backgroundPosition = 'center center';
        }
    }
     function resetImageState(index) { // ... (same as before)
        imageTransforms[index] = { scale: 1, offsetX: 50, offsetY: 50, dataUrl: null };
        photoFiles[index] = null;
        photoImageObjects[index] = null;
        document.getElementById(`scale${index}`).value = 1;
        document.getElementById(`offsetX${index}`).value = 50;
        document.getElementById(`offsetY${index}`).value = 50;
        updatePreviewBackground(index);
        updateGenerateButtonState();
    }

    // --- Event Handlers (Photo Input, Template Input, Slider Changes - Keep as before) ---
     photoInputs.forEach(input => { // ... (same as before)
        input.addEventListener('change', async (event) => {
             const index = parseInt(event.target.dataset.index, 10);
            const file = event.target.files[0];
            if (file) {
                photoFiles[index] = file;
                statusElem.textContent = `Loading photo ${index + 1}...`;
                try {
                    const reader = new FileReader();
                    reader.onload = (e) => {
                        imageTransforms[index].dataUrl = e.target.result;
                        imageTransforms[index].scale = 1;
                        imageTransforms[index].offsetX = 50;
                        imageTransforms[index].offsetY = 50;
                        document.getElementById(`scale${index}`).value = 1;
                        document.getElementById(`offsetX${index}`).value = 50;
                        document.getElementById(`offsetY${index}`).value = 50;
                        updatePreviewBackground(index);
                        updateGenerateButtonState();
                         loadImage(e.target.result)
                            .then(img => {
                                photoImageObjects[index] = img;
                                statusElem.textContent = `Photo ${index + 1} loaded.`;
                                updateGenerateButtonState();
                            })
                            .catch(error => {
                                console.error(error);
                                statusElem.textContent = `Error loading photo ${index + 1}. Please try another file.`;
                                resetImageState(index);
                            });
                    }
                    reader.readAsDataURL(file);
                } catch (error) {
                    console.error("Error reading file:", error);
                    statusElem.textContent = `Error reading photo ${index + 1}.`;
                    resetImageState(index);
                }
            } else {
                resetImageState(index);
            }
        });
    });
    templateInput.addEventListener('change', async (event) => { // ... (same as before)
        const file = event.target.files[0];
        if (file) {
            templateFile = file;
            const reader = new FileReader();
            reader.onload = async (e) => {
                previewTemplate.src = e.target.result;
                previewTemplate.style.display = 'block';
                statusElem.textContent = 'Loading template...';
                try {
                    templateImageObject = await loadImage(e.target.result);
                    statusElem.textContent = 'Template loaded.';
                     // Optional: Automatically set max values for margins based on template size?
                     // gridMarginTopInput.max = templateImageObject.naturalHeight;
                     // gridMarginBottomInput.max = templateImageObject.naturalHeight;
                     // ... etc. (Might be slightly annoying if user already typed)
                } catch (error) {
                    console.error(error);
                    statusElem.textContent = 'Error loading template. Please try another PNG.';
                    templateFile = null; templateImageObject = null;
                    previewTemplate.src = '#'; previewTemplate.style.display = 'none';
                } finally {
                     updateGenerateButtonState();
                }
            }
            reader.readAsDataURL(file);
        } else {
            templateFile = null; templateImageObject = null;
            previewTemplate.src = '#'; previewTemplate.style.display = 'none';
            updateGenerateButtonState();
        }
    });
     function handleSliderChange(event) { // ... (same as before)
        const index = parseInt(event.target.dataset.index, 10);
        const transformType = event.target.id.replace(/[0-9]/g, '');
        if (imageTransforms[index]) {
            imageTransforms[index][transformType] = parseFloat(event.target.value);
            updatePreviewBackground(index);
        }
    }
    scaleSliders.forEach(slider => slider.addEventListener('input', handleSliderChange));
    offsetXSliders.forEach(slider => slider.addEventListener('input', handleSliderChange));
    offsetYSliders.forEach(slider => slider.addEventListener('input', handleSliderChange));

    // --- updateGenerateButtonState (No changes needed) ---
    const updateGenerateButtonState = () => { // ... (same as before)
        const allPhotosReady = photoImageObjects.every(img => img !== null);
        const templateReady = templateImageObject !== null;
        generateBtn.disabled = !(allPhotosReady && templateReady);
        if (!generateBtn.disabled && statusElem.textContent.includes('loaded')) {
             statusElem.textContent = 'Ready to generate.';
        } else if (generateBtn.disabled && (!allPhotosReady || !templateReady)) {
            let missing = [];
            photoImageObjects.forEach((img, i) => { if (!img && photoFiles[i]) missing.push(`Photo ${i+1} (processing)`); else if (!img && !photoFiles[i]) missing.push(`Photo ${i+1}`); });
            if (!templateReady && templateFile) missing.push('Template (processing)');
            else if (!templateReady && !templateFile) missing.push('Template');
            if (missing.length > 0) {
                 statusElem.textContent = `Missing: ${missing.join(', ')}`;
            }
        }
    };

    // --- Main Image Generation Function (MODIFIED) ---
    generateBtn.addEventListener('click', () => {
        statusElem.textContent = 'Generating... Please wait.';
        generateBtn.disabled = true;
        resultImage.style.display = 'none';
        downloadLink.style.display = 'none';

        if (!photoImageObjects.every(img => img) || !templateImageObject) {
            statusElem.textContent = 'Error: Not all images/template are loaded correctly.';
            updateGenerateButtonState();
            return;
        }

        // Use setTimeout to allow the UI to update
        setTimeout(() => {
            try {
                const templateW = templateImageObject.naturalWidth;
                const templateH = templateImageObject.naturalHeight;

                // --- NEW: Read and Validate Grid Margins ---
                const marginTop = parseInt(gridMarginTopInput.value, 10) || 0;
                const marginBottom = parseInt(gridMarginBottomInput.value, 10) || 0;
                const marginLeft = parseInt(gridMarginLeftInput.value, 10) || 0;
                const marginRight = parseInt(gridMarginRightInput.value, 10) || 0;

                // Calculate the actual area for the 2x2 grid
                const gridAreaX = marginLeft;
                const gridAreaY = marginTop;
                const gridAreaWidth = templateW - marginLeft - marginRight;
                const gridAreaHeight = templateH - marginTop - marginBottom;

                // Validate grid area dimensions
                if (gridAreaWidth <= 0 || gridAreaHeight <= 0) {
                    throw new Error(`Invalid grid dimensions calculated. Check margins. Grid Area: ${gridAreaWidth}x${gridAreaHeight}`);
                }

                // Set canvas size based on the template
                canvas.width = templateW;
                canvas.height = templateH;

                // Calculate dimensions for each quadrant *within the grid area*
                const quadWidth = gridAreaWidth / 2;
                const quadHeight = gridAreaHeight / 2;

                 // --- Calculate positions for each quadrant RELATIVE TO CANVAS (0,0) ---
                 const drawPositions = [
                    { x: gridAreaX,              y: gridAreaY },               // Top-Left Quad
                    { x: gridAreaX + quadWidth,  y: gridAreaY },               // Top-Right Quad
                    { x: gridAreaX,              y: gridAreaY + quadHeight },  // Bottom-Left Quad
                    { x: gridAreaX + quadWidth,  y: gridAreaY + quadHeight }   // Bottom-Right Quad
                 ];

                // Clear canvas
                ctx.clearRect(0, 0, canvas.width, canvas.height);

                // --- Draw photos onto the canvas using transforms AND calculated positions/sizes ---
                photoImageObjects.forEach((img, index) => {
                    if (img) {
                        const transform = imageTransforms[index];
                        const pos = drawPositions[index]; // Get the calculated top-left corner
                        // Draw the image into the calculated quadrant space
                        drawImageWithTransform(ctx, img, transform, pos.x, pos.y, quadWidth, quadHeight);
                    }
                });

                // Draw the template overlay on top (covers everything, including margins)
                ctx.drawImage(templateImageObject, 0, 0, canvas.width, canvas.height);

                // Generate the final image URL
                const finalImageDataUrl = canvas.toDataURL('image/png');

                // Display the result
                resultImage.src = finalImageDataUrl;
                resultImage.style.display = 'block';

                // Set up download link
                downloadLink.href = finalImageDataUrl;
                downloadLink.style.display = 'inline-block';

                statusElem.textContent = 'Image generated successfully!';

            } catch (error) {
                console.error("Error during canvas generation:", error);
                statusElem.textContent = `Error: ${error.message || 'Could not generate image.'}`;
            } finally {
                 updateGenerateButtonState();
            }
        }, 50);
    });

    // --- drawImageWithTransform (Keep as before - it works with target rect) ---
    function drawImageWithTransform(ctx, img, transform, targetX, targetY, targetW, targetH) { // ... (same as before)
        const imgWidth = img.naturalWidth;
        const imgHeight = img.naturalHeight;
        const scale = transform.scale;
        const offsetXRatio = transform.offsetX / 100;
        const offsetYRatio = transform.offsetY / 100;

        let sourceW = imgWidth / scale;
        let sourceH = imgHeight / scale;
        let sourceX = (imgWidth - sourceW) * offsetXRatio;
        let sourceY = (imgHeight - sourceH) * offsetYRatio;

        const targetAspect = targetW / targetH;
        const sourceAspect = sourceW / sourceH;

        if (sourceAspect > targetAspect) {
            const newSourceW = sourceH * targetAspect;
            sourceX += (sourceW - newSourceW) * offsetXRatio;
            sourceW = newSourceW;
        } else if (sourceAspect < targetAspect) {
            const newSourceH = sourceW / targetAspect;
            sourceY += (sourceH - newSourceH) * offsetYRatio;
            sourceH = newSourceH;
        }

         sourceX = Math.max(0, Math.min(imgWidth - sourceW, sourceX));
         sourceY = Math.max(0, Math.min(imgHeight - sourceH, sourceY));

        ctx.drawImage(
            img, sourceX, sourceY, sourceW, sourceH,
            targetX, targetY, targetW, targetH
        );
    }

    // --- Initial state check ---
    updateGenerateButtonState();
});