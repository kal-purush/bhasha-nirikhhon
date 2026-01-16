package io.v.vdl;

import com.intellij.navigation.ChooseByNameContributorEx;
import com.intellij.navigation.GotoClassContributor;
import com.intellij.navigation.NavigationItem;
import com.intellij.openapi.project.Project;
import com.intellij.psi.search.GlobalSearchScope;
import com.intellij.psi.stubs.StubIndex;
import com.intellij.psi.stubs.StubIndexKey;
import com.intellij.util.ArrayUtil;
import com.intellij.util.Processor;
import com.intellij.util.indexing.FindSymbolParameters;
import com.intellij.util.indexing.IdFilter;
import io.v.vdl.psi.VdlNamedElement;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class VdlGotoContributorBase<T extends VdlNamedElement> implements GotoClassContributor, ChooseByNameContributorEx {
    protected final StubIndexKey<String, T>[] myIndexKeys;
    @NotNull
    private final Class<T> myClazz;

    public VdlGotoContributorBase(@NotNull Class<T> clazz, @NotNull StubIndexKey<String, T>... key) {
        myIndexKeys = key;
        myClazz = clazz;
    }

    @NotNull
    @Override
    public String[] getNames(@NotNull Project project, boolean includeNonProjectItems) {
        return ArrayUtil.EMPTY_STRING_ARRAY;
    }

    @NotNull
    @Override
    public NavigationItem[] getItemsByName(String name, String pattern, Project project, boolean includeNonProjectItems) {
        return NavigationItem.EMPTY_NAVIGATION_ITEM_ARRAY;
    }

    @Override
    public void processNames(@NotNull Processor<String> processor, @NotNull GlobalSearchScope scope, IdFilter filter) {
        for (StubIndexKey<String, T> key : myIndexKeys) {
            StubIndex.getInstance().processAllKeys(key, processor, scope, filter);
        }
    }

    @Override
    public void processElementsWithName(@NotNull String s,
                                        @NotNull Processor<NavigationItem> processor,
                                        @NotNull FindSymbolParameters parameters) {
        for (StubIndexKey<String, T> key : myIndexKeys) {
            StubIndex.getInstance().processElements(key, s, parameters.getProject(), parameters.getSearchScope(), parameters.getIdFilter(),
                    myClazz, processor);
        }
    }

    @Nullable
    @Override
    public String getQualifiedName(NavigationItem item) {
        if (item instanceof VdlNamedElement) {
            return ((VdlNamedElement)item).getQualifiedName();
        }
        return null;
    }

    @Nullable
    @Override
    public String getQualifiedNameSeparator() {
        // todo[IDEA 15.1]: replace with null
        return ".";
    }
}