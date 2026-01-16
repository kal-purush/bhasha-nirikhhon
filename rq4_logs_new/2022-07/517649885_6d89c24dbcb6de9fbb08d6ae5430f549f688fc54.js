const catalogBtnMore = document.querySelector(".catalog__btn-more");

const catalogProductsContainer = document.querySelector(".catalog__items");

catalogBtnMore.addEventListener("click", () => {
  addProducs();
  selects();
});

const addProducs = () => {
  const catalogProductHTML = `<div class="popular__item popular__item--item-1">
                <div class="popular__item-wrapper">
                  <a href="card.html" class="popular__item-img">
                    <img src="./img/popular/1.png" alt="" />
                  </a>
                  <div class="popular__item-info">
                    <div class="popular__item-row">
                      <div class="popular__item-name">
                        Сухая пенка для умывания
                      </div>

                      <div class="popular__item-select" data-state="">
                        <div
                          class="popular__item-select-default"
                          data-default=""
                        >
                          100 г
                        </div>
                        <div class="popular__item-select-content">
                          <input
                            id="popularItem1"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem"
                            checked
                          />
                          <label
                            for="popularItem1"
                            class="popular__item-select-label"
                            >100 г</label
                          >
                          <input
                            id="popularItem2"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem"
                            data-price=""
                          />
                          <label
                            for="popularItem2"
                            class="popular__item-select-label"
                            >200 г</label
                          >

                          <input
                            id="popularItem3"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem"
                            data-price=""
                          />
                          <label
                            for="popularItem3"
                            class="popular__item-select-label"
                            >300 г</label
                          >
                        </div>
                      </div>
                    </div>
                    <div class="popular__item-row">
                      <p class="popular__item-desc">
                        Состав: белый каолин, фруктовая пудра, пудра шиповника,
                        мягкий ПАВ на основе кокосового масла (рН 6, идеален для
                        кожи), масло косточек абрикоса
                      </p>
                    </div>
                    <div class="popular__item-row">
                      <div class="popular__item-infoprices">
                        <div class="popular__item-price">18 byn</div>
                        <button
                          type="button"
                          class="popular__item-btn"
                        ></button>
                      </div>

                      <button type="button" class="popular__item-wish"></button>
                    </div>
                  </div>
                </div>
              </div>
              <div class="popular__item popular__item--item-2">
                <div class="popular__item-wrapper">
                  <a href="card.html" class="popular__item-img">
                    <img src="./img/popular/2.jpg" alt="" />
                  </a>
                  <div class="popular__item-info">
                    <div class="popular__item-row">
                      <div class="popular__item-name">Гидролат Таволги</div>
                      <div class="popular__item-select" data-state="">
                        <div
                          class="popular__item-select-default"
                          data-default=""
                        >
                          50 мл
                        </div>
                        <div class="popular__item-select-content">
                          <input
                            id="popularItem4"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem2"
                            checked
                          />
                          <label
                            for="popularItem4"
                            class="popular__item-select-label"
                            >50 мл</label
                          >
                          <input
                            id="popularItem5"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem2"
                            data-price=""
                          />
                          <label
                            for="popularItem5"
                            class="popular__item-select-label"
                            >100 мл</label
                          >

                          <input
                            id="popularItem6"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem2"
                            data-price=""
                          />
                          <label
                            for="popularItem6"
                            class="popular__item-select-label"
                            >150 мл</label
                          >
                        </div>
                      </div>
                    </div>
                    <div class="popular__item-row">
                      <p class="popular__item-desc">
                        Состав: мягкий ПАВ на основе кокосового масла (рН 6,
                        идеален для кожи), бетаин, протеины пшеницы, депантенол,
                        композиция эфирных масел
                      </p>
                    </div>
                    <div class="popular__item-row">
                      <div class="popular__item-infoprices">
                        <div class="popular__item-price">15 byn</div>
                        <button
                          type="button"
                          class="popular__item-btn"
                        ></button>
                      </div>

                      <button type="button" class="popular__item-wish"></button>
                    </div>
                  </div>
                </div>
              </div>
              <div class="popular__item popular__item--item-3">
                <div class="popular__item-wrapper">
                  <a href="card.html" class="popular__item-img">
                    <img src="./img/popular/3.png" alt="" />
                  </a>
                  <div class="popular__item-info">
                    <div class="popular__item-row">
                      <div class="popular__item-name">Твердый шампунь</div>
                      <div class="popular__item-select" data-state="">
                        <div
                          class="popular__item-select-default"
                          data-default=""
                        >
                          80 г
                        </div>
                        <div class="popular__item-select-content">
                          <input
                            id="popularItem7"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem3"
                            checked
                          />
                          <label
                            for="popularItem7"
                            class="popular__item-select-label"
                            >80 г</label
                          >
                          <input
                            id="popularItem8"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem3"
                            data-price=""
                          />
                          <label
                            for="popularItem8"
                            class="popular__item-select-label"
                            >100 г</label
                          >

                          <input
                            id="popularItem9"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem3"
                            data-price=""
                          />
                          <label
                            for="popularItem9"
                            class="popular__item-select-label"
                            >150 г</label
                          >
                          <input
                            id="popularItem10"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem3"
                            data-price=""
                          />
                          <label
                            for="popularItem10"
                            class="popular__item-select-label"
                            >200 г</label
                          >
                        </div>
                      </div>
                    </div>
                    <div class="popular__item-row">
                      <p class="popular__item-desc">
                        Состав: омыленные масла оливы, кокоса или натриевые соли
                        натуральных жирных кислот (olivate натрия, кокоат
                        натрия)
                      </p>
                    </div>
                    <div class="popular__item-row">
                      <div class="popular__item-infoprices">
                        <div class="popular__item-price">19 byn</div>
                        <button
                          type="button"
                          class="popular__item-btn"
                        ></button>
                      </div>

                      <button type="button" class="popular__item-wish"></button>
                    </div>
                  </div>
                </div>
              </div>
              <div class="popular__item popular__item--item-8">
                <div class="popular__item-wrapper">
                  <a href="card.html" class="popular__item-img">
                    <img src="./img/popular/8.jpg" alt="" />
                  </a>
                  <div class="popular__item-info">
                    <div class="popular__item-row">
                      <div class="popular__item-name">Твердый шампунь</div>
                      <div class="popular__item-select" data-state="">
                        <div
                          class="popular__item-select-default"
                          data-default=""
                        >
                          80 г
                        </div>
                        <div class="popular__item-select-content">
                          <input
                            id="popularItem24"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem8"
                            checked
                          />
                          <label
                            for="popularItem24"
                            class="popular__item-select-label"
                            >80 г</label
                          >
                          <input
                            id="popularItem25"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem8"
                            data-price=""
                          />
                          <label
                            for="popularItem25"
                            class="popular__item-select-label"
                            >100 г</label
                          >

                          <input
                            id="popularItem26"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem8"
                            data-price=""
                          />
                          <label
                            for="popularItem26"
                            class="popular__item-select-label"
                            >150 г</label
                          >
                          <input
                            id="popularItem27"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem8"
                            data-price=""
                          />
                          <label
                            for="popularItem27"
                            class="popular__item-select-label"
                            >200 г</label
                          >
                        </div>
                      </div>
                    </div>
                    <div class="popular__item-row">
                      <p class="popular__item-desc">
                        Состав: омыленные масла оливы, кокоса или натриевые соли
                        натуральных жирных кислот (olivate натрия, кокоат
                        натрия)
                      </p>
                    </div>
                    <div class="popular__item-row">
                      <div class="popular__item-infoprices">
                        <div class="popular__item-price">19 byn</div>
                        <button
                          type="button"
                          class="popular__item-btn"
                        ></button>
                      </div>

                      <button type="button" class="popular__item-wish"></button>
                    </div>
                  </div>
                </div>
              </div>
              <div class="popular__item popular__item--item-9">
                <div class="popular__item-wrapper">
                  <a href="card.html" class="popular__item-img">
                    <img src="./img/popular/9.jpg" alt="" />
                  </a>
                  <div class="popular__item-info">
                    <div class="popular__item-row">
                      <div class="popular__item-name">Гидролат Таволги</div>
                      <div class="popular__item-select" data-state="">
                        <div
                          class="popular__item-select-default"
                          data-default=""
                        >
                          50 мл
                        </div>
                        <div class="popular__item-select-content">
                          <input
                            id="popularItem28"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem9"
                            checked
                          />
                          <label
                            for="popularItem28"
                            class="popular__item-select-label"
                            >50 мл</label
                          >
                          <input
                            id="popularItem29"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem9"
                            data-price=""
                          />
                          <label
                            for="popularItem29"
                            class="popular__item-select-label"
                            >100 мл</label
                          >

                          <input
                            id="popularItem30"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem9"
                            data-price=""
                          />
                          <label
                            for="popularItem30"
                            class="popular__item-select-label"
                            >150 мл</label
                          >
                        </div>
                      </div>
                    </div>
                    <div class="popular__item-row">
                      <p class="popular__item-desc">
                        Состав: мягкий ПАВ на основе кокосового масла (рН 6,
                        идеален для кожи), бетаин, протеины пшеницы, депантенол,
                        композиция эфирных масел
                      </p>
                    </div>
                    <div class="popular__item-row">
                      <div class="popular__item-infoprices">
                        <div class="popular__item-price">15 byn</div>
                        <button
                          type="button"
                          class="popular__item-btn"
                        ></button>
                      </div>

                      <button type="button" class="popular__item-wish"></button>
                    </div>
                  </div>
                </div>
              </div>
              <div class="popular__item popular__item--item-10">
                <div class="popular__item-wrapper">
                  <a href="card.html" class="popular__item-img">
                    <img src="./img/popular/10.jpg" alt="" />
                  </a>
                  <div class="popular__item-info">
                    <div class="popular__item-row">
                      <div class="popular__item-name">
                        Сухая пенка для умывания
                      </div>

                      <div class="popular__item-select" data-state="">
                        <div
                          class="popular__item-select-default"
                          data-default=""
                        >
                          100 г
                        </div>
                        <div class="popular__item-select-content">
                          <input
                            id="popularItem31"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem10"
                            checked
                          />
                          <label
                            for="popularItem31"
                            class="popular__item-select-label"
                            >100 г</label
                          >
                          <input
                            id="popularItem32"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem10"
                            data-price=""
                          />
                          <label
                            for="popularItem32"
                            class="popular__item-select-label"
                            >200 г</label
                          >

                          <input
                            id="popularItem33"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem10"
                            data-price=""
                          />
                          <label
                            for="popularItem33"
                            class="popular__item-select-label"
                            >300 г</label
                          >
                        </div>
                      </div>
                    </div>
                    <div class="popular__item-row">
                      <p class="popular__item-desc">
                        Состав: мягкий ПАВ на основе кокосового масла (рН 6,
                        идеален для кожи), бетаин, протеины пшеницы, депантенол,
                        композиция эфирных масел
                      </p>
                    </div>
                    <div class="popular__item-row">
                      <div class="popular__item-infoprices">
                        <div class="popular__item-price">18 byn</div>
                        <button
                          type="button"
                          class="popular__item-btn"
                        ></button>
                      </div>

                      <button type="button" class="popular__item-wish"></button>
                    </div>
                  </div>
                </div>
              </div>

              <div class="popular__item popular__item--item-4">
                <div class="popular__item-wrapper">
                  <a href="card.html" class="popular__item-img">
                    <img src="./img/popular/4.jpg" alt="" />
                  </a>
                  <div class="popular__item-info">
                    <div class="popular__item-row">
                      <div class="popular__item-name">Пенка для умывания</div>
                      <div class="popular__item-select" data-state="">
                        <div
                          class="popular__item-select-default"
                          data-default=""
                        >
                          80 мл
                        </div>
                        <div class="popular__item-select-content">
                          <input
                            id="popularItem11"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem4"
                            checked
                          />
                          <label
                            for="popularItem11"
                            class="popular__item-select-label"
                            >80 мл</label
                          >
                          <input
                            id="popularItem12"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem4"
                            data-price=""
                          />
                          <label
                            for="popularItem12"
                            class="popular__item-select-label"
                            >100 мл</label
                          >

                          <input
                            id="popularItem13"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem4"
                            data-price=""
                          />
                          <label
                            for="popularItem13"
                            class="popular__item-select-label"
                            >150 мл</label
                          >
                          <input
                            id="popularItem14"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem4"
                            data-price=""
                          />
                          <label
                            for="popularItem14"
                            class="popular__item-select-label"
                            >200 мл</label
                          >
                          <input
                            id="popularItem15"
                            class="popular__item-select-input"
                            type="radio"
                            name="popularItem4"
                            data-price=""
                          />
                          <label
                            for="popularItem15"
                            class="popular__item-select-label"
                            >250 мл</label
                          >
                        </div>
                      </div>
                    </div>
                    <div class="popular__item-row">
                      <p class="popular__item-desc">
                        Состав: белый каолин, фруктовая пудра, пудра шиповника,
                        мягкий ПАВ на основе кокосового масла (рН 6, идеален для
                        кожи), масло косточек абрикоса
                      </p>
                    </div>
                    <div class="popular__item-row">
                      <div class="popular__item-infoprices">
                        <div class="popular__item-price">20 byn</div>
                        <button
                          type="button"
                          class="popular__item-btn"
                        ></button>
                      </div>

                      <button type="button" class="popular__item-wish"></button>
                    </div>
                  </div>
                </div>
              </div>
              <div class="popular__item popular__item--item-5">
                <span class="popular__item-text">Выгодно!</span>
                <div class="popular__item-inner">
                  <div class="popular__item-wrapper popular__item--item-6">
                    <a href="card.html" class="popular__item-img">
                      <img src="./img/popular/5.jpg" alt="" />
                    </a>
                    <div class="popular__item-info">
                      <div class="popular__item-row">
                        <div class="popular__item-name">
                          Убтан (для умывания)
                        </div>
                        <div class="popular__item-select" data-state="">
                          <div
                            class="popular__item-select-default"
                            data-default=""
                          >
                            80 мл
                          </div>
                          <div class="popular__item-select-content">
                            <input
                              id="popularItem16"
                              class="popular__item-select-input"
                              type="radio"
                              name="popularItem5"
                              checked
                            />
                            <label
                              for="popularItem16"
                              class="popular__item-select-label"
                              >80 мл</label
                            >
                            <input
                              id="popularItem17"
                              class="popular__item-select-input"
                              type="radio"
                              name="popularItem5"
                              data-price=""
                            />
                            <label
                              for="popularItem17"
                              class="popular__item-select-label"
                              >100 мл</label
                            >

                            <input
                              id="popularItem18"
                              class="popular__item-select-input"
                              type="radio"
                              name="popularItem5"
                              data-price=""
                            />
                            <label
                              for="popularItem18"
                              class="popular__item-select-label"
                              >150 мл</label
                            >
                            <input
                              id="popularItem19"
                              class="popular__item-select-input"
                              type="radio"
                              name="popularItem5"
                              data-price=""
                            />
                            <label
                              for="popularItem19"
                              class="popular__item-select-label"
                              >200 мл</label
                            >
                          </div>
                        </div>
                      </div>
                      <div class="popular__item-row">
                        <p class="popular__item-desc">
                          Состав: белый каолин, фруктовая пудра, пудра
                          шиповника, мягкий ПАВ на основе кокосового масла (рН
                          6, идеален для кожи), масло косточек абрикоса
                        </p>
                      </div>
                      <div class="popular__item-row">
                        <div class="popular__item-infoprices">
                          <div class="popular__item-price">15 byn</div>
                          <button
                            type="button"
                            class="popular__item-btn"
                          ></button>
                        </div>

                        <button
                          type="button"
                          class="popular__item-wish"
                        ></button>
                      </div>
                    </div>
                  </div>
                  <div class="popular__item-wrapper popular__item--item-7">
                    <a href="card.html" class="popular__item-img">
                      <img src="./img/popular/6.jpg" alt="" />
                    </a>
                    <div class="popular__item-info">
                      <div class="popular__item-row">
                        <div class="popular__item-name">Тоник Belle skin</div>
                        <div class="popular__item-select" data-state="">
                          <div
                            class="popular__item-select-default"
                            data-default=""
                          >
                            80 мл
                          </div>
                          <div class="popular__item-select-content">
                            <input
                              id="popularItem20"
                              class="popular__item-select-input"
                              type="radio"
                              name="popularItem6"
                              checked
                            />
                            <label
                              for="popularItem20"
                              class="popular__item-select-label"
                              >80 мл</label
                            >
                            <input
                              id="popularItem21"
                              class="popular__item-select-input"
                              type="radio"
                              name="popularItem6"
                              data-price=""
                            />
                            <label
                              for="popularItem21"
                              class="popular__item-select-label"
                              >100 мл</label
                            >

                            <input
                              id="popularItem22"
                              class="popular__item-select-input"
                              type="radio"
                              name="popularItem6"
                              data-price=""
                            />
                            <label
                              for="popularItem22"
                              class="popular__item-select-label"
                              >150 мл</label
                            >
                            <input
                              id="popularItem23"
                              class="popular__item-select-input"
                              type="radio"
                              name="popularItem6"
                              data-price=""
                            />
                            <label
                              for="popularItem23"
                              class="popular__item-select-label"
                              >200 мл</label
                            >
                          </div>
                        </div>
                      </div>
                      <div class="popular__item-row">
                        <div class="popular__item-infoprices">
                          <div class="popular__item-price">11 byn</div>
                          <button
                            type="button"
                            class="popular__item-btn"
                          ></button>
                        </div>

                        <button
                          type="button"
                          class="popular__item-wish"
                        ></button>
                      </div>
                    </div>
                  </div>
                </div>
              </div>`;

  // добавляем в html карточки
  catalogProductsContainer.insertAdjacentHTML("beforeend", catalogProductHTML);
};