/**
 * Admin 管理頁面主程式碼
 * 功能：管理員資訊載入與維護、管理員列表、新增與刪除等操作
 */

document.addEventListener('DOMContentLoaded', () => {
  // 根據網址錨點自動捲動至對應區塊
  const hash = window.location.hash || '#settings';
  const targetElement = document.querySelector(hash);
  if (targetElement) targetElement.scrollIntoView();

  // 初始化下拉選單
  const dropdownElementList = document.querySelectorAll('.dropdown-toggle');
  [...dropdownElementList].forEach(el => new bootstrap.Dropdown(el));

  // 初始載入
  loadAdminInfo();
  loadAllAdmins();

  // 表單事件註冊
  document.getElementById('update-email-form').addEventListener('submit', e => {
    e.preventDefault();
    updateEmail();
  });

  document.getElementById('change-password-form').addEventListener('submit', e => {
    e.preventDefault();
    changePassword();
  });

  document.getElementById('change-username-form').addEventListener('submit', e => {
    e.preventDefault();
    changeUsername();
  });

  document.getElementById('add-admin-form').addEventListener('submit', e => {
    e.preventDefault();
    createAdmin();
  });
});

/**
 * 載入當前管理員資訊
 */
async function loadAdminInfo() {
  try {
    const response = await fetch('/api/admins/current');
    const admin = await response.json();

    if (admin.error) {
      setAdminInfoError();
      return;
    }

    document.getElementById('admin-username').textContent = admin.username;
    document.getElementById('admin-email').textContent = admin.email || '未設置';

    const emailInput = document.getElementById('admin-email-input');
    emailInput.value = admin.email || '';
    emailInput.placeholder = admin.email ? '' : '輸入信箱地址';

    if (admin.last_login) {
      const date = new Date(admin.last_login);
      document.getElementById('admin-last-login').textContent = moment(date).format('YYYY-MM-DD HH:mm:ss');
    } else {
      document.getElementById('admin-last-login').textContent = '首次登錄';
    }
  } catch (error) {
    console.error('載入管理員資訊失敗:', error);
    setAdminInfoError();
  }
}

/**
 * 顯示管理員資訊錯誤提示
 */
function setAdminInfoError() {
  const errorMsg = '獲取資訊失敗';
  document.getElementById('admin-username').textContent = errorMsg;
  document.getElementById('admin-email').textContent = errorMsg;
  document.getElementById('admin-last-login').textContent = errorMsg;
}

/**
 * 更新當前管理員信箱
 */
async function updateEmail() {
  try {
    const email = document.getElementById('admin-email-input').value.trim();
    if (!email) {
      alert('請輸入有效的電子信箱地址');
      return;
    }

    const response = await fetch('/api/admins/email', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email })
    });
    const result = await response.json();

    if (result.success) {
      alert('信箱地址已更新');
      loadAdminInfo();
    } else {
      alert(`更新信箱失敗: ${result.error || '未知錯誤'}`);
    }
  } catch (error) {
    console.error('更新信箱失敗:', error);
    alert('更新信箱失敗');
  }
}

/**
 * 更改當前管理員密碼
 */
async function changePassword() {
  try {
    const currentPassword = document.getElementById('current-password').value;
    const newPassword = document.getElementById('new-password').value;
    const confirmPassword = document.getElementById('confirm-password').value;

    if (!currentPassword || !newPassword || !confirmPassword) {
      alert('所有密碼欄位都必須填寫');
      return;
    }
    if (newPassword !== confirmPassword) {
      alert('新密碼與確認密碼不匹配');
      return;
    }
    if (newPassword.length < 6) {
      alert('新密碼長度至少為6個字元');
      return;
    }

    const response = await fetch('/api/admins/password', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ currentPassword, newPassword })
    });
    const result = await response.json();

    if (result.success) {
      alert('密碼已成功更新');
      document.getElementById('change-password-form').reset();
    } else {
      alert(`更新密碼失敗: ${result.error || '未知錯誤'}`);
    }
  } catch (error) {
    console.error('更改密碼失敗:', error);
    alert('更改密碼失敗');
  }
}

/**
 * 更改當前管理員使用者名稱
 */
async function changeUsername() {
  try {
    const newUsername = document.getElementById('new-username').value.trim();
    if (!newUsername) {
      alert('請輸入新使用者名稱');
      return;
    }
    if (!/^[a-zA-Z0-9_]{3,20}$/.test(newUsername)) {
      alert('使用者名稱只能包含字母、數字和下劃線，長度3-20位');
      return;
    }
    if (!confirm(`確定要將使用者名稱更改為 ${newUsername} 嗎？更改後需要重新登入。`)) {
      return;
    }

    const response = await fetch('/api/admins/username', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ newUsername })
    });
    const result = await response.json();

    if (result.success) {
      alert('使用者名稱更新成功，需要重新登入');
      window.location.href = '/login.html';
    } else {
      alert(`更新使用者名稱失敗: ${result.error || '未知錯誤'}`);
    }
  } catch (error) {
    console.error('更改使用者名稱失敗:', error);
    alert('更改使用者名稱失敗');
  }
}

/**
 * 載入所有管理員資料
 */
async function loadAllAdmins() {
  try {
    const adminList = document.getElementById('admin-list');
    adminList.innerHTML = '<tr><td colspan="5" class="text-center">載入中...</td></tr>';

    const res = await fetch('/api/admins');
    const data = await res.json();
    if (!data || !Array.isArray(data)) {
      adminList.innerHTML = '<tr><td colspan="5" class="text-danger">載入失敗</td></tr>';
      return;
    }
    if (data.length === 0) {
      adminList.innerHTML = '<tr><td colspan="5" class="text-center">無管理員</td></tr>';
      return;
    }

    const currentAdminRes = await fetch('/api/admins/current');
    const currentAdmin = await currentAdminRes.json();
    const currentUsername = currentAdmin.username;

    adminList.innerHTML = '';
    data.forEach(admin => {
      const isCurrentAdmin = admin.username === currentUsername;
      const deleteButton = isCurrentAdmin
        ? `<button class="btn btn-sm btn-danger" disabled title="無法刪除當前登入的管理員">
            <i class="bi bi-trash"></i> 刪除
          </button>`
        : `<button class="btn btn-sm btn-danger" onclick="deleteAdmin(${admin.id}, '${admin.username}')">
            <i class="bi bi-trash"></i> 刪除
          </button>`;

      adminList.innerHTML += `
        <tr>
          <td>${admin.id}</td>
          <td>${admin.username}</td>
          <td>${admin.email || '-'}</td>
          <td>
            ${admin.is_disabled
              ? '<span class="badge bg-danger">已停用</span>'
              : '<span class="badge bg-success">已啟用</span>'}
          </td>
          <td>
            <button class="btn btn-sm btn-warning" onclick="toggleAdminStatus(${admin.id}, '${admin.username}', ${admin.is_disabled})">
              ${admin.is_disabled ? '啟用' : '停用'}
            </button>
            <button class="btn btn-sm btn-info" onclick="changeOtherAdminPassword(${admin.id}, '${admin.username}')">
              修改密碼
            </button>
            ${deleteButton}
          </td>
        </tr>
      `;
    });
  } catch (error) {
    console.error('載入管理員失敗:', error);
    document.getElementById('admin-list').innerHTML =
      `<tr><td colspan="5" class="text-danger">載入失敗: ${error.message}</td></tr>`;
  }
}

/**
 * 切換指定管理員的啟用 / 停用狀態
 * @param {number} adminId
 * @param {string} username
 * @param {boolean} isDisabled
 */
async function toggleAdminStatus(adminId, username, isDisabled) {
  if (!confirm(`確定要${isDisabled ? '啟用' : '停用'}管理員 "${username}" 嗎？`)) return;

  try {
    const response = await fetch(`/api/admins/${adminId}/toggle-status`, { method: 'POST' });
    const result = await response.json();

    if (result.success) {
      alert(`已${isDisabled ? '啟用' : '停用'}管理員 "${username}"`);
      refreshAdminList();
    } else {
      alert(result.error || '操作失敗');
    }
  } catch (error) {
    console.error('切換管理員狀態失敗:', error);
    alert('切換管理員狀態失敗');
  }
}

/**
 * 更改其他管理員密碼
 * @param {number} adminId
 * @param {string} username
 */
async function changeOtherAdminPassword(adminId, username) {
  const newPass = prompt(`請輸入要修改的密碼（管理員 "${username}"）:`);
  if (!newPass) return;

  if (newPass.length < 6) {
    alert('密碼長度至少為6個字元');
    return;
  }

  try {
    const response = await fetch(`/api/admins/${adminId}/password`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ newPassword: newPass })
    });
    const result = await response.json();

    if (result.success) {
      alert(`已更新管理員 "${username}" 的密碼`);
    } else {
      alert(result.error || '修改失敗');
    }
  } catch (error) {
    console.error('修改密碼失敗:', error);
    alert('修改密碼失敗');
  }
}

/**
 * 新增管理員
 */
async function createAdmin() {
  try {
    const username = document.getElementById('new-admin-username').value.trim();
    const password = document.getElementById('new-admin-password').value;
    const email = document.getElementById('new-admin-email').value.trim();

    if (!username) {
      alert('請輸入使用者名稱');
      return;
    }
    if (!/^[a-zA-Z0-9_]{3,20}$/.test(username)) {
      alert('使用者名稱只能包含字母、數字和下劃線，長度3-20位');
      return;
    }
    if (!password) {
      alert('請輸入密碼');
      return;
    }
    if (password.length < 6) {
      alert('密碼長度至少為6個字元');
      return;
    }

    const response = await fetch('/api/admins', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password, email })
    });
    const result = await response.json();

    if (result.success) {
      alert('新管理員創建成功');
      document.getElementById('add-admin-form').reset();
      refreshAdminList();
    } else {
      alert(`創建失敗: ${result.error || '未知錯誤'}`);
    }
  } catch (error) {
    console.error('創建管理員失敗:', error);
    alert('創建管理員失敗');
  }
}

/**
 * 刪除管理員
 * @param {number} adminId
 * @param {string} username
 */
async function deleteAdmin(adminId, username) {
  if (!confirm(`確定要刪除管理員 "${username}" 嗎？此操作無法撤銷！`)) return;

  try {
    const response = await fetch(`/api/admins/${adminId}`, { method: 'DELETE' });
    const result = await response.json();

    if (result.success) {
      alert(`已成功刪除管理員 "${username}"`);
      refreshAdminList();
    } else {
      alert(result.error || '刪除失敗');
    }
  } catch (error) {
    console.error('刪除管理員失敗:', error);
    alert('刪除管理員失敗');
  }
}

/**
 * 重新載入管理員列表
 */
function refreshAdminList() {
  loadAllAdmins();
}

// 將需要被HTML直接調用的函數綁定到全域 (不改動原先HTML的 onclick 寫法)
window.loadAllAdmins = loadAllAdmins;
window.toggleAdminStatus = toggleAdminStatus;
window.changeOtherAdminPassword = changeOtherAdminPassword;
window.deleteAdmin = deleteAdmin;
window.refreshAdminList = refreshAdminList;