document.addEventListener("DOMContentLoaded", function () {
    const dropZone = document.getElementById("puzzle-container");
    const imageUrl = "image/ft.jpg"; // Ganti dengan gambar puzzle
    const gridSize = 3; // Ukuran 3x3
    let correctPieces = 0;
    let piecesContainer;

    function createPuzzleBoard() {
        // Mengatur ukuran grid puzzle sesuai ukuran layar
        const puzzleWidth = window.innerWidth < 768 ? 240 : 310; // Lebar grid lebih kecil pada mobile
        const puzzleHeight = window.innerWidth < 768 ? 240 : 310; // Tinggi grid lebih kecil pada mobile
        dropZone.style.width = `${puzzleWidth}px`;
        dropZone.style.height = `${puzzleHeight}px`;

        for (let i = 0; i < gridSize * gridSize; i++) {
            const slot = document.createElement("div");
            slot.classList.add("puzzle-slot");
            slot.dataset.index = i;
            slot.addEventListener("dragover", allowDrop);
            slot.addEventListener("drop", dropPiece);
            dropZone.appendChild(slot);
        }
    }

    function createPuzzlePieces() {
        piecesContainer = document.createElement("div");
        piecesContainer.classList.add("pieces-container");

        // Menyesuaikan ukuran potongan puzzle
        const pieceSize = window.innerWidth < 768 ? 80 : 100; // Potongan puzzle lebih kecil pada mobile

        for (let i = 0; i < gridSize * gridSize; i++) {
            const piece = document.createElement("div");
            piece.classList.add("puzzle-piece");
            piece.style.backgroundImage = `url(${imageUrl})`;
            piece.style.backgroundPosition = `-${(i % gridSize) * pieceSize}px -${Math.floor(i / gridSize) * pieceSize}px`;
            piece.draggable = true;
            piece.dataset.index = i;
            piece.addEventListener("dragstart", dragStart);
            piecesContainer.appendChild(piece);
        }

        document.querySelector(".puzzle").appendChild(piecesContainer);
    }

    function dragStart(event) {
        event.dataTransfer.setData("text", event.target.dataset.index);
        event.dataTransfer.setData("background", event.target.style.backgroundImage);
        event.dataTransfer.setData("position", event.target.style.backgroundPosition);
    }

    function allowDrop(event) {
        event.preventDefault();
    }

    function dropPiece(event) {
        event.preventDefault();
        const droppedIndex = event.dataTransfer.getData("text");
        const backgroundImage = event.dataTransfer.getData("background");
        const backgroundPosition = event.dataTransfer.getData("position");

        if (event.target.classList.contains("puzzle-slot") && event.target.childElementCount === 0) {
            const placedPiece = document.createElement("div");
            placedPiece.classList.add("puzzle-piece");
            placedPiece.style.backgroundImage = backgroundImage;
            placedPiece.style.backgroundPosition = backgroundPosition;
            event.target.appendChild(placedPiece);

            // Hapus gambar dari container setelah ditempatkan
            const pieceToRemove = piecesContainer.querySelector(`.puzzle-piece[data-index='${droppedIndex}']`);
            if (pieceToRemove) {
                piecesContainer.removeChild(pieceToRemove);
            }

            if (parseInt(event.target.dataset.index) === parseInt(droppedIndex)) {
                correctPieces++;
                if (correctPieces === gridSize * gridSize) {
                    displayBalloons(); // Ganti alert dengan efek balon terbang
                }
            }
        }
    }

    function displayBalloons() {
        const balloonContainer = document.createElement("div");
        balloonContainer.classList.add("balloon-container");
        document.body.appendChild(balloonContainer);

        // Posisi balon yang lebih tepat (dari bawah puzzle)
        balloonContainer.style.bottom = "50px"; // Menyesuaikan posisi balon agar muncul tepat di bawah puzzle

        // Fungsi untuk membuat balon
        function createBalloon() {
            const balloon = document.createElement("div");
            balloon.classList.add("balloon");

            // Menetapkan posisi acak untuk balon dari bagian bawah puzzle
            balloon.style.left = `${Math.random() * 100}vw`;
            balloon.style.animationDuration = `${Math.random() * 5 + 5}s`; // Durasi acak antara 5 sampai 10 detik

            // Menambahkan balon ke container
            balloonContainer.appendChild(balloon);

            // Hapus balon setelah animasi selesai
            balloon.addEventListener("animationend", function() {
                balloon.remove();
            });
        }

        // Membuat 20 balon sekaligus
        for (let i = 0; i < 20; i++) {
            createBalloon();
        }

        // Membuat balon setiap 1 detik
        setInterval(createBalloon, 1000);
    }

    createPuzzleBoard();
    createPuzzlePieces();
});