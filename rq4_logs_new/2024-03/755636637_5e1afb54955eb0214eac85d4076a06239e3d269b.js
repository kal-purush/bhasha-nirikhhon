import React from 'react'
import s from '../styles/Page.module.css'

const LearnPath = () => {
	return (
		<div className={s.main}>
			<div className={s.c1}>
				<p className={s.main_text}>Траектория обучения </p>
			</div>
			<div className={s.c2}>
				<div className={s.c2_1}>
					<p className={s.Code}>09.03.02 Информационные системы и технологии</p>
				</div>
				<div className={s.c2_2}>
					<p class={s.Code}>
						<span>09.03.02</span>
						<br />
						Фуллстек разработка
					</p>
				</div>
				<div className={s.c2A}>
					<div className={s.c2_3}>
						<p class={s.Code}>09.03.hjkg 02 Компьютерный дизайн</p>
					</div>
					<div className={s.c2_4}>
						<p class={s.Code}>11.03.04 Электроника и наноэлектроника</p>
					</div>
				</div>
				<div className={s.c2B}>
					<div className={s.c2_5}>
						<p class={s.Code}>
							<span>12.03.05</span>
							<br />
							Лазерная техника и лазерные технологии
						</p>
					</div>
					<div className={s.c2_6}>
						<p class={s.Code}>15.03.01 Машиностроение</p>
					</div>
				</div>
				<div className={s.c2B}>
					<div className={s.c2_5}>
						<p class={s.Code}>
							<span class={s.first_line}>22.03.01 </span>
							<br />
							Материаловедение и технологии материалов
						</p>
					</div>
					<div className={s.c2_6}>
						<p class={s.Code}>27.03.01 Стандартизация и метрология</p>
					</div>
				</div>
				<div className={s.c2B}>
					<div className={s.c2_5}>
						<p class={s.Code}>
							<span class={s.first_line}>28.03.01 </span>
							<br />
							Нанотехнологии и микросистемная техника
						</p>
					</div>
					<div className={s.c2_6}>
						<p class={s.Code}>
							<span class={s.first_line}>29.03.04 </span>
							<br />
							Технология художественной обработки материалов
						</p>
					</div>
				</div>
				<div className={s.c2B}>
					<div className={s.c2_5}>
						<p class={s.Code}>
							<span class={s.first_line}>54.03.01 </span>
							<br />
							Дизайн
						</p>
					</div>
					<div className={s.c2_6}>
						<p class={s.Code}>
							<span class={s.first_line}>12.05.01 </span>
							<br />
							Электронные и оптико-электронные приборы
						</p>
					</div>
				</div>
			</div>
		</div>
	)
}

export default LearnPath