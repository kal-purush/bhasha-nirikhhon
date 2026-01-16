package com.app.colibri.registry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.app.colibri.controller.WordController;
import com.app.colibri.model.Word;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

@Data
public class TagRegistry implements Serializable {
	private static final long serialVersionUID = 543191549900073090L;

	private AtomicInteger maxID = new AtomicInteger(0);

	private SortedMap<Integer, String> idTagMap = new TreeMap<>();
	@JsonIgnore
	private SortedMap<String, Integer> tagIdMap = new TreeMap<>();

	public TagRegistry() {
	};

	public int putTag(String p_tag) {
		String v_tag = p_tag == null ? "" : p_tag.toUpperCase().trim();

		if (v_tag.equals("")) {
			return 0;
		}

		if (!tagIdMap.containsKey(v_tag)) {
			int v_newID = maxID.incrementAndGet();

			idTagMap.put(v_newID, v_tag);
			tagIdMap.put(v_tag, v_newID);
		}

		return obtainIdByTag(v_tag);
	}

	public int obtainIdByTag(String p_tag) {
		String v_tag = p_tag == null ? "" : p_tag.toUpperCase().trim();
		return tagIdMap.getOrDefault(v_tag, 0);
	}

	public String obtainTagById(int p_id) {
		return idTagMap.getOrDefault(p_id, "");
	}

	public boolean isWordListContainTag(List<Word> p_wordList, String p_tag) {
		return isWordListContainTag(p_wordList, obtainIdByTag(p_tag));
	}

	public boolean isWordListContainTag(List<Word> p_wordList, int p_tagID) {
		for (Word v_word : p_wordList) {
			if (isWordContainTag(v_word, p_tagID)) {
				return true;
			}
		}

		return false;
	}

	public boolean isWordContainTag(Word p_word, String p_tag) {
		return isWordContainTag(p_word, obtainIdByTag(p_tag));
	}

	public boolean isWordContainTag(Word p_word, int p_tagID) {
		return p_word.getTagIdSet().contains(p_tagID);
	}

	public void removeTag(String p_tag) {
		removeTag(obtainIdByTag(p_tag));
	}

	public void removeTag(int p_tagID) {
		if (!isWordListContainTag(WordController.allWordsList, p_tagID)) {
			tagIdMap.remove(obtainTagById(p_tagID));
			idTagMap.remove(p_tagID);
		}
	}

	public void removeTagForce(String p_tag) {
		removeTagForce(obtainIdByTag(p_tag));
	}

	public void removeTagForce(int p_tagID) {
		WordController.allWordsList.forEach(v_word -> v_word.removeTag(p_tagID));

		tagIdMap.remove(obtainTagById(p_tagID));
		idTagMap.remove(p_tagID);
	}

	public void removeAllTags() {
		for (int v_tagID : new HashSet<>(idTagMap.keySet())) {
			removeTag(v_tagID);
		}
	}

	public void removeAllTagsForce() {
		for (int v_tagID : new HashSet<>(idTagMap.keySet())) {
			removeTagForce(v_tagID);
		}
	}

	public void restoreTagIdMap() {
		tagIdMap.clear();
		idTagMap.forEach((k, v) -> tagIdMap.put(v, k));
	}

	public List<String> obtainTagList() {
		return fillTagList(new ArrayList<String>());
	}

	public List<String> fillTagList(List<String> p_tagList) {
		tagIdMap.keySet().forEach(p_tagList::add);
		return p_tagList;
	}

	public void editTag(int p_tagID, String p_newTag) {
		editTag(obtainTagById(p_tagID), p_newTag);
	}

	public void editTag(String p_oldTag, String p_newTag) {
		p_oldTag = p_oldTag == null ? "" : p_oldTag.toUpperCase().trim();
		p_newTag = p_newTag == null ? "" : p_newTag.toUpperCase().trim();

		if (!(p_oldTag.equals("") || p_newTag.equals(""))) {
			int v_tagID = obtainIdByTag(p_oldTag);

			if (v_tagID > 0 && obtainIdByTag(p_newTag) == 0) {
				idTagMap.replace(v_tagID, p_newTag);
				tagIdMap.remove(p_oldTag);
				tagIdMap.put(p_newTag, v_tagID);
			}
		}
	}

}