using Microsoft.AspNetCore.Mvc;

public class DeviceCategoryController : Controller
{
    public IActionResult DeviceCategory()
    {
        var categories = InMemoryDatabase.DeviceCategories;
        return View(categories);
    }

    public IActionResult AddEdit(int? id)
    {
        var category = id.HasValue
            ? InMemoryDatabase.DeviceCategories.FirstOrDefault(c => c.CategoryId == id.Value)
            : new DeviceCategory();
        return View(category);
    }

    [HttpPost]
    public IActionResult AddEdit(DeviceCategory category)
    {
        if (ModelState.IsValid)
        {
            if (category.CategoryId == 0) 
            {
                category.CategoryId = InMemoryDatabase.GetNextId("DeviceCategory");
                InMemoryDatabase.DeviceCategories.Add(category);
            }
            else 
            {
                var existingCategory = InMemoryDatabase.DeviceCategories.FirstOrDefault(c => c.CategoryId == category.CategoryId);
                if (existingCategory != null)
                {
                    existingCategory.CategoryName = category.CategoryName;
                }
            }

            return RedirectToAction("DeviceCategory", "DeviceCategory");
        }

        return View(category);
    }

    public IActionResult Delete(int id)
    {
        var category = InMemoryDatabase.DeviceCategories.FirstOrDefault(c => c.CategoryId == id);
        if (category != null)
        {
            InMemoryDatabase.DeviceCategories.Remove(category);
        }
        return RedirectToAction("DeviceCategory", "DeviceCategory");
    }
}