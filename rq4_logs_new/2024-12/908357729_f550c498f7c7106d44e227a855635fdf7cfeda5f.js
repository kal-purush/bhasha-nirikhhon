document.addEventListener('DOMContentLoaded', () => {
    const expenses = JSON.parse(localStorage.getItem('expenses')) || [];
    const addExpenseBtn = document.getElementById('add-expense-btn');
    const addSplitBtn = document.getElementById('add-split-btn');
    const splitContainer = document.getElementById('split-container');
    const expensesTable = document.getElementById('expenses-table').querySelector('tbody');
    const totalExpenses = document.getElementById('total-expenses');
    const expenseDateInput = document.getElementById('expense-date');
    const expenseCategorySelect = document.getElementById('expense-category');
    const editModal = document.getElementById('edit-modal');
    const closeModalBtn = document.getElementById('close-modal-btn');
    const modalBody = document.querySelector('.modal-body');

    // تعيين تاريخ اليوم كقيمة افتراضية
    const today = new Date().toISOString().split('T')[0];
    expenseDateInput.value = today;

    // إضافة تقسيم جديد
    addSplitBtn.addEventListener('click', () => {
        const splitItem = document.createElement('div');
        splitItem.classList.add('split-item');
        splitItem.innerHTML = `
    <input type="text" class="split-name" placeholder="مثال: عنصر جديد">
    <input type="number" class="split-amount" placeholder="المبلغ">
    <button class="remove-split">حذف</button>
`;
        splitContainer.appendChild(splitItem);

        splitItem.querySelector('.remove-split').addEventListener('click', () => {
            splitItem.remove();
        });
    });

    // إضافة مصروف جديد
    addExpenseBtn.addEventListener('click', () => {
        const name = document.getElementById('expense-name').value.trim();
        const amount = parseFloat(document.getElementById('expense-amount').value);
        const date = document.getElementById('expense-date').value;
        const description = document.getElementById('expense-description').value.trim();
        const category = expenseCategorySelect.value;

        // التحقق من صحة المدخلات
        if (!name || !amount || isNaN(amount) || amount <= 0) {
            displayMessage('يرجى إدخال اسم العملية والمبلغ بشكل صحيح.', 'error');
            return;
        }

        const splits = [];
        let totalSplitAmount = 0;

        document.querySelectorAll('.split-item').forEach(item => {
            const splitName = item.querySelector('.split-name').value.trim();
            const splitAmount = parseFloat(item.querySelector('.split-amount').value);
            if (splitName && splitAmount && splitAmount > 0) {
                splits.push({ name: splitName, amount: splitAmount });
                totalSplitAmount += splitAmount; // جمع المبالغ لكل التقسيمات
            }
        });

        // التحقق إذا كانت هناك تقسيمات وكان مجموعها لا يساوي المبلغ الكلي
        if (splits.length > 0 && totalSplitAmount !== amount) {
            displayMessage('مجموع مبالغ التقسيمات يجب أن يساوي المبلغ الكلي للعملية.', 'error');
            return;
        }

        const newExpense = {
            name,
            amount,
            date,
            description,
            category,
            splits
        };

        expenses.push(newExpense);
        saveExpenses();
        renderExpenses();
        resetForm();  // إعادة تعيين المدخلات بعد إضافة المصروف
        displayMessage('تم إضافة المصروف بنجاح!', 'success');
    });

    // إعادة تعيين النموذج
    function resetForm() {
        // إعادة تعيين القيم الافتراضية
        document.getElementById('expense-name').value = ''; // إفراغ حقل اسم العملية
        document.getElementById('expense-amount').value = ''; // إفراغ حقل المبلغ
        document.getElementById('expense-description').value = ''; // إفراغ حقل الوصف
        document.getElementById('expense-date').value = new Date().toISOString().split('T')[0]; // إعادة تعيين التاريخ
        document.getElementById('expense-category').value = 'غذاء'; // التصنيف الافتراضي

        // التأكد من وجود الحقل الافتراضي لتفاصيل المبلغ
        const splitContainer = document.getElementById('split-container');

        // حذف كل الحقول الإضافية، والإبقاء على الحقل الأول فقط
        ffff

        // إذا كان هناك عنصر أول موجود في الحاوية، إفراغه أو إعادة تعيينه
        const defaultSplitItem = splitContainer.firstElementChild;  // استخدم `firstElementChild` بدلاً من `firstChild`

        if (defaultSplitItem && defaultSplitItem.classList.contains('split-item')) {
            // إفراغ القيم في الحقل الأول
            defaultSplitItem.querySelector('.split-name').value = ''; // إفراغ اسم العنصر
            defaultSplitItem.querySelector('.split-amount').value = ''; // إفراغ المبلغ
        } else {
            // إذا لم يكن هناك تقسيم افتراضي، أضفه
            const newSplitItem = document.createElement('div');
            newSplitItem.classList.add('split-item');
            newSplitItem.innerHTML = `
                <input type="text" class="split-name" placeholder="مثال: مياه">
                <input type="number" class="split-amount" placeholder="المبلغ">`;
            splitContainer.appendChild(newSplitItem);
        }
    }




    // عرض الرسائل
    function displayMessage(message, type) {
        const messageElement = document.createElement('div');
        messageElement.classList.add('message', type);
        messageElement.textContent = message;
        document.body.appendChild(messageElement);

        setTimeout(() => {
            messageElement.remove();
        }, 3000);
    }

    // حفظ المصاريف في Local Storage
    function saveExpenses() {
        localStorage.setItem('expenses', JSON.stringify(expenses));
    }

    // عرض المصاريف في الجدول
    function renderExpenses() {
        expensesTable.innerHTML = '';
        let total = 0;
        expenses.forEach(expense => {
            const row = document.createElement('tr');

            // تحقق إذا كان هناك تفاصيل تقسيم
            const splitsDetails = expense.splits && expense.splits.length > 0
                ? expense.splits.map(split => `${split.name}: ${split.amount}`).join('<br>')
                : 'لا توجد تفاصيل';

            // التأكد من أن expense.amount رقم
            const amount = parseFloat(expense.amount) || 0;

            row.innerHTML = `
        <td>${expense.name}</td>
        <td>${expense.date}</td>
        <td>${amount.toFixed(2)}</td>
        <td>${splitsDetails}</td>
        <td>${expense.description}</td>
        <td>${expense.category}</td>
        <td class="actions">
            <button class="edit-btn" onclick="editExpense(${expenses.indexOf(expense)})">تعديل</button>
            <button class="delete-btn" onclick="deleteExpense(${expenses.indexOf(expense)})">حذف</button>
        </td>
    `;
            expensesTable.appendChild(row);
            total += amount;
        });

        totalExpenses.textContent = total.toFixed(2);
    }

    // إغلاق نافذة التعديل
    closeModalBtn.addEventListener('click', () => {
        editModal.style.display = 'none';
    });

    // إضافة المصاريف إلى الجدول عند تحميل الصفحة
    renderExpenses();

    // تعديل المصروف
    window.editExpense = function (index) {
        const expense = expenses[index];
        modalBody.innerHTML = `
    <label for="edit-expense-name">اسم العملية:</label>
    <input type="text" id="edit-expense-name" value="${expense.name}">
    <label for="edit-expense-amount">المبلغ الكلي:</label>
    <input type="number" id="edit-expense-amount" value="${expense.amount}">
    <label for="edit-expense-date">التاريخ:</label>
    <input type="date" id="edit-expense-date" value="${expense.date}">
    <label for="edit-expense-category">التصنيف:</label>
    <select id="edit-expense-category">
        <option value="غذاء" ${expense.category === 'غذاء' ? 'selected' : ''}>غذاء</option>
        <option value="مواصلات" ${expense.category === 'مواصلات' ? 'selected' : ''}>مواصلات</option>
        <option value="ترفيه" ${expense.category === 'ترفيه' ? 'selected' : ''}>ترفيه</option>
        <option value="فواتير" ${expense.category === 'فواتير' ? 'selected' : ''}>فواتير</option>
    </select>
    <label for="edit-expense-description">الوصف:</label>
    <textarea id="edit-expense-description">${expense.description}</textarea>
    <button id="save-edit-expense-btn">حفظ التعديلات</button>
`;
        editModal.style.display = 'flex';

        document.getElementById('save-edit-expense-btn').addEventListener('click', () => {
            const editedName = document.getElementById('edit-expense-name').value.trim();
            const editedAmount = parseFloat(document.getElementById('edit-expense-amount').value);
            const editedDate = document.getElementById('edit-expense-date').value;
            const editedCategory = document.getElementById('edit-expense-category').value;
            const editedDescription = document.getElementById('edit-expense-description').value.trim();

            if (!editedName || isNaN(editedAmount) || editedAmount <= 0) {
                displayMessage('يرجى إدخال بيانات صحيحة.', 'error');
                return;
            }

            expense.name = editedName;
            expense.amount = editedAmount;
            expense.date = editedDate;
            expense.category = editedCategory;
            expense.description = editedDescription;

            saveExpenses();
            renderExpenses();
            editModal.style.display = 'none';
            displayMessage('تم حفظ التعديلات!', 'success');
        });
    }

    // حذف المصروف
    window.deleteExpense = function (index) {
        expenses.splice(index, 1);
        saveExpenses();
        renderExpenses();
        displayMessage('تم حذف المصروف!', 'error');
    }
});

