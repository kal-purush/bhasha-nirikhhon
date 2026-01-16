/*
 *  Copyright (c) 2016 RoboSwag (Gavriil Sitnikov, Vsevolod Ivanov)
 *
 *  This file is part of RoboSwag library.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package ru.touchin.roboswag.core.observables.storable.concrete;

import android.support.annotation.NonNull;
import android.support.annotation.Nullable;

import java.util.List;

import ru.touchin.roboswag.core.observables.storable.Converter;
import ru.touchin.roboswag.core.observables.storable.Migration;
import ru.touchin.roboswag.core.observables.storable.Storable;
import ru.touchin.roboswag.core.observables.storable.Store;
import rx.Observable;

/**
 * Created by Gavriil Sitnikov on 20/12/2016.
 * List wrapper of {@link Storable}.
 *
 * @param <TKey>         Type of key to identify object;
 * @param <TItemObject>  Type of items in actual list object;
 * @param <TStoreObject> Type of store object.
 */
@SuppressWarnings({"unchecked", "CPD-START"})
public class ListStorable<TKey, TItemObject, TStoreObject> {

    @NonNull
    private final Storable<TKey, List, TStoreObject> storable;

    public ListStorable(@NonNull final Storable<TKey, List, TStoreObject> storable) {
        this.storable = storable;
    }

    /**
     * Wraps {@link Storable#get()}.
     */
    @NonNull
    public Observable<List<TItemObject>> get() {
        return storable.get().map(list -> (List<TItemObject>) list);
    }

    /**
     * Wraps {@link Storable#set(Object)}.
     */
    @NonNull
    public Observable<?> set(@Nullable final List<TItemObject> list) {
        return storable.set(list);
    }

    /**
     * Wraps {@link Storable#setCalm(Object)}.
     */
    public void setCalm(@Nullable final List<TItemObject> list) {
        storable.setCalm(list);
    }

    /**
     * Wraps {@link Storable#getSync()}.
     */
    @Nullable
    public List<TItemObject> getSync()
            throws Store.StoreException, Converter.ConversionException, Migration.MigrationException {
        return (List<TItemObject>) storable.getSync();
    }

    /**
     * Wraps {@link Storable#setSync(Object)}.
     */
    public void setSync(@Nullable final List<TItemObject> list)
            throws Store.StoreException, Converter.ConversionException, Migration.MigrationException {
        storable.setSync(list);
    }

}