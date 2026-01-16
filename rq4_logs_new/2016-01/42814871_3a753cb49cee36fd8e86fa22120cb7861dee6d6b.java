package ru.furnituranatali.app;

/**
 * Класс, описывающий константы, общие для всего приложения
 */
public class Const {
    /**
     * Значения для намерений, передаваемых сервису
     * Использование:
     * intent ACTION_UPDATE, ExtraData String EXTRA_ALL.
     * intent ACTION_UPDATE, ExtraData String EXTRA_LEVEL, ExtraData long parentElem
     *  (parentElem - код елемента-родителя для искомого уровня)
     * intent ACTION_UPDATE, ExtraData String EXTRA_ELEMENT, ExtraData long codeElem
     *  (codeElem - код искомого елемента)
     */
    public static final String ACTION_UPDATE = "ru.furnituranatali.app.action.UPDATE";
    public static final String ACTION_TIMER_FINISH = "ru.furnituranatali.app.action.TIMER_FINISH";

    public static final String EXTRA_NAME_UPD_TARGET = "ru.furnituranatali.app.extra.name.UPDATE_TARGET";
    public static final String EXTRA_NAME_CODE_OF_ELEM = "ru.furnituranatali.app.extra.name.CODE_OF_ELEMENT";
    public static final String EXTRA_VAL_ALL = "ru.furnituranatali.app.extra.val.ALL";
    public static final String EXTRA_VAL_LEVEL = "ru.furnituranatali.app.extra.val.LEVEL";
    public static final String EXTRA_VAL_ELEMENT = "ru.furnituranatali.app.extra.val.ELEMENT";
    public static final String EXTRA_NAME_MESSAGE = "ru.furnituranatali.app.extra.name.MESSAGE";
    public static final String EXTRA_NAME_SVC_FINISH = "ru.furnituranatali.app.extra.name.SVC_FINISH";
    public static final String EXTRA_NAME_SVC_WAITTIME = "ru.furnituranatali.app.extra.name.SVC_WAITTIME";
    public static final String EXTRA_VAL_SUCCESS = "ru.furnituranatali.app.extra.val.SUCCESS";
    public static final String EXTRA_VAL_ERROR = "ru.furnituranatali.app.extra.val.ERROR";
    public static final String EXTRA_VAL_FINISH = "ru.furnituranatali.app.extra.val.FINISH";

}