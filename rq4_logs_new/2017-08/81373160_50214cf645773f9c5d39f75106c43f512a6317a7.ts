import { reducer } from './index';

describe('reducers', () => {
  it('Should handle Load Recipes action', () => {
    let state;
    state = reducer({
      recipes: [{
        id: 3,
        title: '',
        image_path: '',
        rating: 4.5,
        description: '',
        prep_time: '',
        portions: 6,
        calories: 465
      }]
    }, {
      type: '[Recipes] Load Recipes'
    });

    expect(state).toEqual({
      recipes: [{
        id: 3,
        title: '',
        image_path: '',
        rating: 4.5,
        description: '',
        prep_time: '',
        portions: 6,
        calories: 465
      }]
    });
  });

  it('Shuold handle Load Success action', () => {
    let state;
    state = reducer({
      recipes: [{
        id: 3,
        title: '',
        image_path: '',
        rating: 4.5,
        description: '',
        prep_time: '',
        portions: 6,
        calories: 465
      }]
    }, {
      payload: [{
        id: 1,
        title: 'מרק ריבולטה טוסקני',
        image_path: 'data/recipes/img/ribollita.jpg',
        rating: 4,
        // tslint:disable-next-line
        description: 'ריבולטה הוא המרק הטוסקני המפורסם ביותר שהלכה למעשה מדובר בנזיד המורכב מירקות ולחם. השפית לירז שדה (מבראסרי עין כרם) מציעה את הגרסה הירושלמית',
        prep_time: '2.5',
        portions: 8,
        calories: 460
      }, {
        id: 2,
        title: 'מרק ירקות איסלנדי טבעוני ליום חורפי',
        image_path: 'data/recipes/img/iceland_vegan.jpg',
        rating: 3,
        description: 'כל טעמי החורף בצלחת: ירקות, פטריות וגם הדרים. מתכון למרק קל ומחמם לכבוד החורף שבפתח עם כל הירקות שאפשר למצוא בשוק',
        prep_time: '1.5',
        portions: 6,
        calories: 372
      }, {
        id: 3,
        title: 'דושפרה: מרק בוכרי שהוא ארוחה שלמה',
        image_path: 'data/recipes/img/dushpara.jpg',
        rating: 4.5,
        // tslint:disable-next-line
        description: 'דושפרה הוא אחד המרקים הנפוצים והמוכרים במטבח הבוכרי. כמרק מחמם ומשביע הוא מציע, מעבר לכיסונים שמעניקים לו את שמו, גם ירקות רבים, קטניות וכדורי בשר קטנים',
        prep_time: '2.5',
        portions: 6,
        calories: 465
      }, {
        id: 4,
        title: 'מרק פרמז\'ן צלול: הפינוק שחיפשתם לחורף',
        image_path: 'data/recipes/img/parmigiano.jpg',
        rating: 4,
        description: 'מרק גבינת פרמז\'ן לבן, עשיר בטעם ורחוק ככל האפשר מההגדרה "כבד"',
        prep_time: '1.5',
        portions: 8,
        calories: 640
      }, {
        id: 5,
        title: 'מרק חרירה טבעוני של השף אריק שנק',
        image_path: 'data/recipes/img/harira.jpg',
        rating: 3,
        description: 'גרסה קצת אחרת למרק חרירה של השף אריק שנק',
        prep_time: '1',
        portions: 8,
        calories: 420
      }, {
        id: 6,
        title: 'מרק גבעולי סלרי',
        image_path: 'data/recipes/img/celery.jpg',
        rating: 4,
        description: 'מרק סלרי שהגבעולים הפריכים משמשים בו כמעין קרוטונים',
        prep_time: '2.25',
        portions: 4,
        calories: 370
      }, {
        id: 7,
        title: 'מרק ירוקים טבעוני עם שום שחור ובצל ירוק',
        image_path: 'data/recipes/img/greens_vegan.jpg',
        rating: 0,
        description: 'אספו את כל הירוקים שאתם מוצאים בשוק - שעועית, ברוקולי, קייל, אפונה והכניסו אותם למרק טבעוני מפתיע',
        prep_time: '2.5',
        portions: 4,
        calories: 320
      }, {
        id: 8,
        title: 'מרק דגים תאילנדי עם שיטאקי, חצילים וצ\'ילי',
        image_path: 'data/recipes/img/thai_soup.jpg',
        rating: 5,
        description: 'מהטעימים שבמרקים התאילנדיים. אם משתמשים בשיטאקי מיובשות, חולטים אותן טרם השימוש בקערה עם מים רותחים',
        prep_time: '2',
        portions: 4,
        calories: 400
      }, {
        id: 9,
        title: 'אז מה הסיפור הגדול סביב הראמן?',
        image_path: 'data/recipes/img/raman.jpg',
        rating: 4,
        // tslint:disable-next-line
        description: 'הראמן במקור הוא אוכל סיני שאומץ על ידי היפנים בחום וכמיטב המסורת היפנית, שודרג והגיע לשיאים חדשים. אז מה העניין סביב מרק האטריות הזה? מה הדבר הכי חשוב בו? ואיך יצרו היפנים מרק שגורם לך להתגעגע? השף ברק נדב נסע ליפן וחזר עם התובנות ועם המתכון המקורי להרפתקנים בלבד',
        prep_time: '3',
        portions: 6,
        calories: 640
      }, {
        id: 10,
        title: 'איך נהפך מרק הלחם האיטלקי ממאכל של עניים - לאמנות',
        image_path: 'data/recipes/img/ribollita_toscana.jpg',
        rating: 5,
        // tslint:disable-next-line
        description: 'הפפה־דה־פומודורו והריבוליטה נוצרו כמאכלים משביעים בעבור כפריים עניים. למרות פשטות חומרי הגלם והשימוש בשאריות לחם, ואולי דווקא בזכותם, הם הפכו למרקים מסעירים',
        prep_time: '1.5',
        portions: 10,
        calories: 520
      }
    ],
    type: '[Recipes] Load Recipes Success'
  });

  expect(state).toEqual({
    recipes: [
      {
        id: 1,
        title: 'מרק ריבולטה טוסקני',
        image_path: 'data/recipes/img/ribollita.jpg',
        rating: 4,
        // tslint:disable-next-line
        description: 'ריבולטה הוא המרק הטוסקני המפורסם ביותר שהלכה למעשה מדובר בנזיד המורכב מירקות ולחם. השפית לירז שדה (מבראסרי עין כרם) מציעה את הגרסה הירושלמית',
        prep_time: '2.5',
        portions: 8,
        calories: 460
      }, {
        id: 2,
        title: 'מרק ירקות איסלנדי טבעוני ליום חורפי',
        image_path: 'data/recipes/img/iceland_vegan.jpg',
        rating: 3,
        description: 'כל טעמי החורף בצלחת: ירקות, פטריות וגם הדרים. מתכון למרק קל ומחמם לכבוד החורף שבפתח עם כל הירקות שאפשר למצוא בשוק',
        prep_time: '1.5',
        portions: 6,
        calories: 372
      }, {
        id: 3,
        title: 'דושפרה: מרק בוכרי שהוא ארוחה שלמה',
        image_path: 'data/recipes/img/dushpara.jpg',
        rating: 4.5,
        // tslint:disable-next-line
        description: 'דושפרה הוא אחד המרקים הנפוצים והמוכרים במטבח הבוכרי. כמרק מחמם ומשביע הוא מציע, מעבר לכיסונים שמעניקים לו את שמו, גם ירקות רבים, קטניות וכדורי בשר קטנים',
        prep_time: '2.5',
        portions: 6,
        calories: 465
      }, {
        id: 4,
        title: 'מרק פרמז\'ן צלול: הפינוק שחיפשתם לחורף',
        image_path: 'data/recipes/img/parmigiano.jpg',
        rating: 4,
        description: 'מרק גבינת פרמז\'ן לבן, עשיר בטעם ורחוק ככל האפשר מההגדרה "כבד"',
        prep_time: '1.5',
        portions: 8,
        calories: 640
      }, {
        id: 5,
        title: 'מרק חרירה טבעוני של השף אריק שנק',
        image_path: 'data/recipes/img/harira.jpg',
        rating: 3,
        description: 'גרסה קצת אחרת למרק חרירה של השף אריק שנק',
        prep_time: '1',
        portions: 8,
        calories: 420
      }, {
        id: 6,
        title: 'מרק גבעולי סלרי',
        image_path: 'data/recipes/img/celery.jpg',
        rating: 4,
        description: 'מרק סלרי שהגבעולים הפריכים משמשים בו כמעין קרוטונים',
        prep_time: '2.25',
        portions: 4,
        calories: 370
      }, {
        id: 7,
        title: 'מרק ירוקים טבעוני עם שום שחור ובצל ירוק',
        image_path: 'data/recipes/img/greens_vegan.jpg',
        rating: 0,
        description: 'אספו את כל הירוקים שאתם מוצאים בשוק - שעועית, ברוקולי, קייל, אפונה והכניסו אותם למרק טבעוני מפתיע',
        prep_time: '2.5',
        portions: 4,
        calories: 320
      }, {
        id: 8,
        title: 'מרק דגים תאילנדי עם שיטאקי, חצילים וצ\'ילי',
        image_path: 'data/recipes/img/thai_soup.jpg',
        rating: 5,
        description: 'מהטעימים שבמרקים התאילנדיים. אם משתמשים בשיטאקי מיובשות, חולטים אותן טרם השימוש בקערה עם מים רותחים',
        prep_time: '2',
        portions: 4,
        calories: 400
      }, {
        id: 9,
        title: 'אז מה הסיפור הגדול סביב הראמן?',
        image_path: 'data/recipes/img/raman.jpg',
        rating: 4,
        // tslint:disable-next-line
        description: 'הראמן במקור הוא אוכל סיני שאומץ על ידי היפנים בחום וכמיטב המסורת היפנית, שודרג והגיע לשיאים חדשים. אז מה העניין סביב מרק האטריות הזה? מה הדבר הכי חשוב בו? ואיך יצרו היפנים מרק שגורם לך להתגעגע? השף ברק נדב נסע ליפן וחזר עם התובנות ועם המתכון המקורי להרפתקנים בלבד',
        prep_time: '3',
        portions: 6,
        calories: 640
      }, {
        id: 10,
        title: 'איך נהפך מרק הלחם האיטלקי ממאכל של עניים - לאמנות',
        image_path: 'data/recipes/img/ribollita_toscana.jpg',
        rating: 5,
        // tslint:disable-next-line
        description: 'הפפה־דה־פומודורו והריבוליטה נוצרו כמאכלים משביעים בעבור כפריים עניים. למרות פשטות חומרי הגלם והשימוש בשאריות לחם, ואולי דווקא בזכותם, הם הפכו למרקים מסעירים',
        prep_time: '1.5',
        portions: 10,
        calories: 520
      }
    ]
  });
});
});