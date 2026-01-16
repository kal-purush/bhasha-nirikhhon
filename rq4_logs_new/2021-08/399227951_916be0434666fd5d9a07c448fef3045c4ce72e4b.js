function moveUpShopsList(event)
{
    const shopsListBlock = document.querySelector('.all-shops');

    if (!shopsListBlock.classList.contains('upped'))
    {
        event.currentTarget.setAttribute('aria-label', 'Скрыть карту');
    }
    else
    {
        event.currentTarget.setAttribute('aria-label', 'Показать карту');
    }

    event.currentTarget.classList.toggle('rotated');
    shopsListBlock.classList.toggle('upped');
}

document.addEventListener('DOMContentLoaded', e => {
    document.querySelector('button.up-shops-list').addEventListener('click', moveUpShopsList);
})