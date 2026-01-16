package com.yodean.oa.sys.dictionary.core;

import com.yodean.oa.sys.dictionary.entity.Word;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

/**
 * Created by rick on 6/4/18.
 */
@Component
public class StringToWordConverter implements Converter<String, Word> {


    @Override
    public Word convert(String s) {
      return DictionaryUtils.string2Word(s);
    }
}