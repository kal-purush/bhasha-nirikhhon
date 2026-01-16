let produkData = JSON.parse(localStorage.getItem("produkData")) || [];

// Muat Kategori dan Supplier ke Dropdown
function muatProduk() {
    const kategoriDropdown = document.getElementById("kategoriProduk");
    const supplierDropdown = document.getElementById("supplierProduk");

    const kategoriData = JSON.parse(localStorage.getItem("kategoriData")) || [];
    const supplierData = JSON.parse(localStorage.getItem("supplierData")) || [];

    kategoriDropdown.innerHTML = `<option value="" disabled selected>Pilih kategori</option>`;
    supplierDropdown.innerHTML = `<option value="" disabled selected>Pilih supplier</option>`;

    kategoriData.forEach((kategori) => {
        kategoriDropdown.innerHTML += `<option value="${kategori.nama}">${kategori.nama}</option>`;
    });

    supplierData.forEach((supplier) => {
        supplierDropdown.innerHTML += `<option value="${supplier.nama}">${supplier.nama}</option>`;
    });
}

// Tambah Produk
document.getElementById("produkForm").addEventListener("submit", function (e) {
    e.preventDefault();

    const namaProduk = document.getElementById("namaProduk").value;
    const kategoriProduk = document.getElementById("kategoriProduk").value;
    const supplierProduk = document.getElementById("supplierProduk").value;

    const newProduk = {
        id: `PROD${Date.now()}`,
        nama: namaProduk,
        kategori: kategoriProduk,
        supplier: supplierProduk,
    };

    produkData.push(newProduk);
    localStorage.setItem("produkData", JSON.stringify(produkData));

    tampilkanAlert("Produk berhasil ditambahkan!", "success");
    tampilkanProduk();
    document.getElementById("produkForm").reset();
});

// Tampilkan Produk
function tampilkanProduk() {
    const produkTable = document.getElementById("produkTable");
    produkTable.innerHTML = "";

    produkData.forEach((produk) => {
        produkTable.innerHTML += `
            <tr>
                <td>${produk.id}</td>
                <td>${produk.nama}</td>
                <td>${produk.kategori}</td>
                <td>${produk.supplier}</td>
                <td>
                    <button class="btn btn-warning btn-sm" onclick="editProduk('${produk.id}')"><i class="fas fa-edit"></i> Edit</button>
                    <button class="btn btn-danger btn-sm" onclick="hapusProduk('${produk.id}')"><i class="fas fa-trash"></i> Hapus</button>
                </td>
            </tr>
        `;
    });
}

// Edit Produk
function editProduk(id) {
    const produk = produkData.find((p) => p.id === id);
    if (produk) {
        document.getElementById("namaProduk").value = produk.nama;
        document.getElementById("kategoriProduk").value = produk.kategori;
        document.getElementById("supplierProduk").value = produk.supplier;
        hapusProduk(id); // Hapus data lama
    }
}

// Hapus Produk
function hapusProduk(id) {
    produkData = produkData.filter((p) => p.id !== id);
    localStorage.setItem("produkData", JSON.stringify(produkData));
    tampilkanAlert("Produk berhasil dihapus!", "danger");
    tampilkanProduk();
}

// Tampilkan Alert
function tampilkanAlert(message, type) {
    const alertContainer = document.getElementById("alertContainer");
    alertContainer.innerHTML = `
        <div class="alert alert-${type} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        </div>
    `;

    setTimeout(() => {
        alertContainer.innerHTML = "";
    }, 3000);
}

// Muat data saat halaman pertama kali dimuat
muatProduk();
tampilkanProduk();