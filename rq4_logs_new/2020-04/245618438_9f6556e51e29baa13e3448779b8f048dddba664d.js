ymaps.ready(init);
function init(){
    // Создание карты.
    var myMap = new ymaps.Map("map", {
        // Координаты центра карты.
        // Порядок по умолчанию: «широта, долгота».
        // Чтобы не определять координаты центра карты вручную,
        // воспользуйтесь инструментом Определение координат.
        center: [56.502510, 84.984000],
        // Уровень масштабирования. Допустимые значения:
        // от 0 (весь мир) до 19.
        zoom: 12,
        controls: []
    });

    const coords = [  
    [56.488343, 84.978978],
    [56.455831, 84.973754],
    ];

    const myCollection = new ymaps.GeoObjectCollection({
        draggable: false,
        iconLayout: 'default#image',
        iconImageHref: './images/icons/map-marker.svg',
        conImageSize: [15, 27],
        iconImageOffset: [-15, -27]
    });

    coords.forEach(coord => {
        myCollection.add(new ymaps.Placemark(coord))
    });
  
    var zoomControl = new ymaps.control.ZoomControl({
        options: {
            size: "small"
        }
    });

myMap.behaviors.disable('scrollZoom');    
myMap.controls.add(zoomControl);
    myMap.geoObjects.add(myCollection);
}