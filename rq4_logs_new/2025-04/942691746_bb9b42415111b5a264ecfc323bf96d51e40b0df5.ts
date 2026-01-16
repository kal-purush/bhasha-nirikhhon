import Web3 from "web3";

document.addEventListener('DOMContentLoaded', async () => {
    const userProfile = document.getElementById('userProfile')!;
    const userName = document.getElementById('userName')!;
    const nameInput = document.getElementById('nameInput') as HTMLInputElement;
    const tooltip = document.getElementById('tooltip')!;
    const shortAddress = document.getElementById('shortAddress')!;
    const balanceElement = document.getElementById('balance')!;
    const logoutButton = document.getElementById('logoutButton')!;
    const avatar = document.getElementById('avatar')!;

    let web3: Web3 | undefined;
    if ((window as any).ethereum) {
        web3 = new Web3((window as any).ethereum);
    }

    const getPlayer = async () => {
        try {
            const response = await fetch('http://localhost:7272/api/v1/player');
            if (!response.ok) throw new Error('Không thể lấy thông tin người chơi');
            const data = await response.json();
            const token = sessionStorage.getItem("token");
            return data.some((player: any) => player.token === token);
        } catch (error) {
            console.error("Lỗi khi lấy thông tin người chơi:", error);
            alert("Đã xảy ra lỗi khi lấy thông tin người chơi. Vui lòng thử lại.");
        }
    };

    const refreshBalance = async () => {
        if (web3) {
            try {
                const userAddress = sessionStorage.getItem('userAddress');
                if (userAddress) {
                    const weiBalance = await web3.eth.getBalance(userAddress);
                    const ethBalance = web3.utils.fromWei(weiBalance, 'ether');
                    const balance = parseFloat(ethBalance).toFixed(4);
                    balanceElement.textContent = balance;
                    sessionStorage.setItem('userBalance', balance);
                }
            } catch (error) {
                console.error("Error refreshing balance:", error);
            }
        }
    };

    const loadUserData = () => {
        const storedName = sessionStorage.getItem('userName');
        const storedAddress = sessionStorage.getItem('shortAddress');
        const storedBalance = sessionStorage.getItem('userBalance');
        const storedInitial = sessionStorage.getItem('userInitial');
        const fullAddress = sessionStorage.getItem('userAddress');

        if (storedName && storedAddress) {
            userName.textContent = storedName;
            shortAddress.textContent = storedAddress;
            balanceElement.textContent = storedBalance || '0';

            avatar.textContent = storedInitial || storedName.charAt(0).toUpperCase();

            userProfile.style.display = 'flex';

            if (fullAddress) {
                refreshBalance();
                removeWalletData(fullAddress);
            }

            sessionStorage.removeItem('userBalance');
            sessionStorage.removeItem('shortAddress');
            sessionStorage.removeItem('userName');
            sessionStorage.removeItem('userInitial');
            sessionStorage.removeItem('userAddress');
        } else {
            window.location.href = "../index.html";
        }
    };

    const handleLogout = () => {
        sessionStorage.setItem('loggedOut', 'true');
        sessionStorage.clear();
        window.location.href = "../index.html";
    };

    const removeWalletData = (address: string | null) => {
        if (address) {
            localStorage.removeItem(`walletName_${address}`);
            console.log(`Đã xóa dữ liệu ví: walletName_${address}`);
        }
    };

    const handleEditName = () => {
        userName.style.display = 'none';
        nameInput.style.display = 'block';
        nameInput.value = userName.textContent || '';
        nameInput.focus();
    };

    const handleSaveName = () => {
        const newName = nameInput.value.trim();
        if (newName !== '') {
            const userAddress = sessionStorage.getItem('userAddress');
            if (userAddress) {
                localStorage.setItem(`walletName_${userAddress}`, newName);
                userName.textContent = newName;
                avatar.textContent = newName.charAt(0).toUpperCase();
                sessionStorage.setItem('userName', newName);
                sessionStorage.setItem('userInitial', newName.charAt(0).toUpperCase());
            }
        }
        nameInput.style.display = 'none';
        userName.style.display = 'block';
    };

    logoutButton.addEventListener('click', handleLogout);
    userName.addEventListener('dblclick', handleEditName);
    nameInput.addEventListener('blur', handleSaveName);
    nameInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') handleSaveName();
    });

    userProfile.addEventListener('mouseenter', () => {
        tooltip.style.display = 'block';
        setTimeout(() => {
            tooltip.style.transform = 'translateY(0)';
            tooltip.style.opacity = '1';
        }, 50);
    });

    userProfile.addEventListener('mouseleave', () => {
        tooltip.style.transform = 'translateY(10px)';
        tooltip.style.opacity = '0';
        setTimeout(() => {
            tooltip.style.display = 'none';
        }, 300);
    });

    loadUserData();
});