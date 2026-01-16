package comics.com.app.data.mapper;

import android.support.annotation.NonNull;
import android.support.annotation.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import comics.com.app.data.pojo.ComicCreator;
import comics.com.app.data.pojo.ComicCreators;
import comics.com.app.data.pojo.ComicPrice;
import comics.com.app.domain.entities.Price;

/**
 * Created by alessandro.candolini on 27/06/2017.
 */

public class AuthorsMapper {

    @Inject
    public AuthorsMapper() {
    }

    @Nullable
    List<String> convert(@NonNull ComicCreators comicCreators) {

        if (comicCreators.getItems() != null && comicCreators.getItems() .size() > 0 ) {
            final List<String> authors = new ArrayList<>();
            for (ComicCreator creator : comicCreators.getItems()) {
                String name = creator.getName();
                if ( name != null ) {
                    authors.add(name);
                }
            }
            return authors;
        }

        return null;

    }
}