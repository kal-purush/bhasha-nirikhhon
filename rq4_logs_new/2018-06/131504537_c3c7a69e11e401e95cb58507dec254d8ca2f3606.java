package org.openqa.selenium.support;




import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.openqa.selenium.By;

import framework.ConfigurationManager;

public abstract class AbstractFindByBuilder {

  public abstract By buildIt(Object annotation, Field field);

  protected By buildByFromFindBy(FindBy findBy) {
    assertValidFindBy(findBy);

    By ans = buildByFromShortFindBy(findBy);
    if (ans == null) {
      ans = buildByFromLongFindBy(findBy);
    }

    return ans;
  }

  protected By buildByFromShortFindBy(FindBy findBy) {
    if (!"".equals(findBy.className())) {
      return By.className(findBy.className());
    }

    if (!"".equals(findBy.css())) {
      return By.cssSelector(findBy.css());
    }

    if (!"".equals(findBy.id())) {
      return By.id(findBy.id());
    }

    if (!"".equals(findBy.linkText())) {
      return By.linkText(findBy.linkText());
    }

    if (!"".equals(findBy.name())) {
      return By.name(findBy.name());
    }

    if (!"".equals(findBy.partialLinkText())) {
      return By.partialLinkText(findBy.partialLinkText());
    }

    if (!"".equals(findBy.tagName())) {
      return By.tagName(findBy.tagName());
    }

    if (!"".equals(findBy.xpath())) {
      return By.xpath(findBy.xpath());
    }

	if (!"".equals(findBy.locator())) {
		System.out.println(findBy.locator());
		return getBy(findBy.locator());
	}

    // Fall through
    return null;
  }

  protected By buildByFromLongFindBy(FindBy findBy) {
    return findBy.how().buildBy(findBy.using());
  }

  protected void assertValidFindBys(FindBys findBys) {
    for (FindBy findBy : findBys.value()) {
      assertValidFindBy(findBy);
    }
  }

  protected void assertValidFindBy(FindBy findBy) {
    if (findBy.how() != null) {
      if (findBy.using() == null) {
        throw new IllegalArgumentException(
            "If you set the 'how' property, you must also set 'using'");
      }
    }

    Set<String> finders = new HashSet<String>();
    if (!"".equals(findBy.using())) finders.add("how: " + findBy.using());
    if (!"".equals(findBy.className())) finders.add("class name:" + findBy.className());
    if (!"".equals(findBy.css())) finders.add("css:" + findBy.css());
    if (!"".equals(findBy.id())) finders.add("id: " + findBy.id());
    if (!"".equals(findBy.linkText())) finders.add("link text: " + findBy.linkText());
    if (!"".equals(findBy.name())) finders.add("name: " + findBy.name());
    if (!"".equals(findBy.partialLinkText()))
      finders.add("partial link text: " + findBy.partialLinkText());
    if (!"".equals(findBy.tagName())) finders.add("tag name: " + findBy.tagName());
    if (!"".equals(findBy.xpath())) finders.add("xpath: " + findBy.xpath());
    if (!"".equals(findBy.locator())) finders.add("xpath: " + findBy.locator());

    // A zero count is okay: it means to look by name or id.
    if (finders.size() > 1) {
      throw new IllegalArgumentException(
          String.format("You must specify at most one location strategy. Number found: %d (%s)",
                        finders.size(), finders.toString()));
    }
  }

  protected void assertValidFindAll(FindAll findBys) {
    for (FindBy findBy : findBys.value()) {
      assertValidFindBy(findBy);
    }
  }
  
  public By getBy(String locator) {
		System.out.println("locator : " + locator);
		locator = ConfigurationManager.getBundle().getString(locator, locator);
		System.out.println("locator : " + locator);
		String[] parts = locator.split("=", 2);
		By by = null;
		System.out.println("parts[0]");
		switch (parts[0].trim()) {
		case "xpath":
			by = By.xpath(parts[1]);
			break;
		case "name":
			by = By.name(parts[1]);
			break;
		case "link":
			by = By.linkText(parts[1]);
			break;
		case "id":
			by = By.id(parts[1]);
			break;
		case "css":
			by = By.cssSelector(parts[1]);
			break;
		default:
			throw new RuntimeException("invalid locator");
		}
		return by;
	}

}