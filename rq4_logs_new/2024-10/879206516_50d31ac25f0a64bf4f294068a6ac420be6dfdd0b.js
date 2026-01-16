 document.addEventListener('DOMContentLoaded', function () {
            const paginationLinks = document.querySelectorAll('.ajax-pagination');

            paginationLinks.forEach(function (link) {
                link.addEventListener('click', function (e) {
                    e.preventDefault(); // Предотвращаем переход по ссылке

                    const page = this.getAttribute('data-page');
                    if (!page) return; // Если нет номера страницы, выходим

                    // Выполняем AJAX-запрос
                    fetch('<?php echo admin_url('admin-ajax.php'); ?>?action=load_page&paged=' + page)
                        .then(response => response.text())
                        .then(data => {
                            // Заменяем содержимое на новой странице
                            document.querySelector('.article__list').innerHTML = data;

                            // Обновляем состояние пагинации
                            updatePagination(parseInt(page), <?php echo $query->max_num_pages; ?>);
                        })
                        .catch(error => console.error('Ошибка:', error));
                });
            });

            // Функция обновления пагинации
            function updatePagination(currentPage, totalPages) {
                const paginationItems = document.querySelectorAll('.article__pagination li');

                paginationItems.forEach(item => {
                    const pageLink = item.querySelector('a');
                    if (pageLink) {
                        const page = parseInt(pageLink.getAttribute('data-page'));
                        if (page === currentPage) {
                            item.classList.add('active'); // Устанавливаем активный класс для текущей страницы
                        } else {
                            item.classList.remove('active');
                        }
                    }
                });
            }
        });
