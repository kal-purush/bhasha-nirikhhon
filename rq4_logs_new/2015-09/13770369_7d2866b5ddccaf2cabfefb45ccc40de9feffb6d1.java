package alonedroid.com.nanitabe;

import android.support.v4.app.Fragment;
import android.text.TextUtils;

import alonedroid.com.nanitabe.scene.choice.NtChoiceFragment;
import alonedroid.com.nanitabe.scene.history.NtHistoryFragment;
import alonedroid.com.nanitabe.scene.search.NtSearchFragment;
import alonedroid.com.nanitabe.scene.select.NtSelectFragment;
import alonedroid.com.nanitabe.scene.top.NtTopFragment;
import alonedroid.com.nanitabe.scene.uratop.NtUraTopFragment;

public class NtRouter {

    private static final String SEPARATOR = "\001";

    private static final String TRUE = "1";

    private static final String CHOICE_URASEARCH = "ura_search";

    public static String getTopMap() {
        return mapping(NtTopFragment.class.getSimpleName());
    }

    public static String getUraTopMap() {
        return mapping(NtUraTopFragment.class.getSimpleName());
    }

    public static String getSelectMap() {
        return mapping(NtSelectFragment.class.getSimpleName());
    }

    public static String getHistoryMap() {
        return mapping(NtHistoryFragment.class.getSimpleName());
    }

    public static String getChoiceMap(String recipes) {
        return mapping(NtChoiceFragment.class.getSimpleName(), recipes, TRUE, TRUE);
    }

    public static String getUraSearchMap(String query) {
        return mapping(NtChoiceFragment.class.getSimpleName(), CHOICE_URASEARCH, query);
    }

    public static String getTopSearchMap(String query) {
        return mapping(NtSearchFragment.class.getSimpleName(), query, "");
    }

    public static String getRecipeOpenMap(String id) {
        return mapping(NtSearchFragment.class.getSimpleName(), "", id);
    }

    private static String mapping(String name, String... params) {
        StringBuffer sb = new StringBuffer(name);
        for (String param : params) {
            sb.append(SEPARATOR).append(param);
        }
        return sb.toString();
    }

    public static Fragment route(String map) {
        String[] params = map.split(SEPARATOR);
        if (NtChoiceFragment.class.getSimpleName().equals(params[0])) {
            return getChoiceFragment(params);
        } else if (NtTopFragment.class.getSimpleName().equals(params[0])) {
            return NtTopFragment.newInstance();
        } else if (NtUraTopFragment.class.getSimpleName().equals(params[0])) {
            return NtUraTopFragment.newInstance();
        } else if (NtSelectFragment.class.getSimpleName().equals(params[0])) {
            return NtSelectFragment.newInstance();
        } else if (NtHistoryFragment.class.getSimpleName().equals(params[0])) {
            return NtHistoryFragment.newInstance();
        } else if (NtSearchFragment.class.getSimpleName().equals(params[0])) {
            if (TextUtils.isEmpty(params[1])) {
                return NtSearchFragment.newOpenInstance(params[2]);
            } else {
                return NtSearchFragment.newSearchInstance(params[1]);
            }
        }
        return null;
    }

    private static Fragment getChoiceFragment(String[] params) {
        if (CHOICE_URASEARCH.equals(params[1])) {
            return NtChoiceFragment.newSearchInstance(params[2]);
        } else {
        }
        return NtChoiceFragment.newInstance(params[1].split(","));
    }

    private static boolean isTrue(String bln) {
        return TRUE.equals(bln);
    }
}